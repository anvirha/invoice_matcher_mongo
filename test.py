# -*- coding: utf-8 -*-
"""
Advanced GST Reconciliation Engine with Score-Based Matching & Multithreading
-----------------------------------------------------------------------------

This script performs a sophisticated reconciliation between internal expense records
and government 2B tax data. It uses a multi-layered, score-based approach to
handle common real-world data issues.

Key Features:
- Multithreading for high-performance processing.
- Score-based matching engine considering multiple data points.
- Handles swapped/interchanged GSTINs (Hotel GSTIN vs. Client GSTIN).
- Fuzzy matching (RapidFuzz) for both GSTINs and Invoice Numbers to correct
  parsing errors (e.g., 'O' vs '0', 'I' vs '1').
- Prioritizes perfect matches but gracefully falls back to proximity-based
  searches (date, amount) if needed.
- Calculates a final confidence score for every potential match.
- Robust handling of data types, null values, and date formats.
- Clear separation of configuration, utilities, and core logic.
"""
import re
import os
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any, Optional, Tuple

from pymongo import MongoClient, UpdateOne
from rapidfuzz import fuzz  # type: ignore
from tqdm import tqdm

# ==============================================================================
# === CONFIGURATION
# ==============================================================================

# --- MongoDB Configuration ---
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = "test"

# --- Reconciliation Thresholds & Weights ---
# The engine will find candidates within these wider ranges for scoring
DATE_PROXIMITY_DAYS = 15
AMOUNT_PROXIMITY_PCT = 0.15  # 15% tolerance for finding candidates

# --- Scoring Configuration ---
# Weights for calculating the final confidence score (must sum to 1.0)
WEIGHT_GSTIN = 0.35
WEIGHT_INVOICE = 0.45
WEIGHT_DATE = 0.10
WEIGHT_AMOUNT = 0.10

# Minimum fuzzy match scores to be considered valid
GSTIN_FUZZY_THRESHOLD = 93
INVOICE_FUZZY_THRESHOLD = 93

# Minimum confidence score to be considered a "Confirmed Match"
CONFIRMED_MATCH_THRESHOLD = 85

# --- Performance ---
MAX_WORKERS = os.cpu_count() or 4 # Number of parallel threads

# ==============================================================================
# === HELPER UTILITIES
# ==============================================================================

def clean_gstin(gstin: Optional[str]) -> str:
    """Cleans a GSTIN by removing special chars, stripping, and uppercasing."""
    if not isinstance(gstin, str) or not gstin:
        return ""
    return re.sub(r"[^0-9A-Z]", "", gstin.strip().upper())

def clean_invoice_number(inum: Optional[str]) -> str:
    """Cleans an invoice number for reliable comparison."""
    if not isinstance(inum, str) or not inum:
        return ""
    # Remove special chars, uppercase, and then strip leading zeros
    # which are common inconsistencies.
    cleaned = re.sub(r"[^0-9A-Z]", "", inum.strip().upper())
    return cleaned.lstrip("0")

def parse_date(date_val: Any) -> Optional[datetime]:
    """Safely parses date values which can be strings or mongo's '$date' dict."""
    if not date_val:
        return None
    if isinstance(date_val, datetime):
        return date_val
        
    date_str = ""
    if isinstance(date_val, dict) and "$date" in date_val:
        date_str = date_val["$date"]
    elif isinstance(date_val, str):
        date_str = date_val

    if not date_str:
        return None

    try:
        # Handle formats like "2024-06-06T00:00:00.000+00:00" or "...Z"
        return datetime.fromisoformat(date_str.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None

# ==============================================================================
# === CORE RECONCILIATION ENGINE
# ==============================================================================

class ReconciliationProcessor:
    """
    Orchestrates the entire GST reconciliation process using a score-based,
    multi-threaded approach.
    """

    def __init__(self):
        self.client = MongoClient(MONGO_URI)
        self.db = self.client[DB_NAME]
        self.parsed_data = self.db["sap_expense"]
        self.two_b_data = self.db["two_b"]
        print("MongoDB connection established.")
        self._ensure_indexes()

    def _ensure_indexes(self):
        """Ensures optimal indexes exist on the 2B collection for fast lookups."""
        self.two_b_data.create_index([("ctin", 1)], name="ctin_idx")
        self.two_b_data.create_index([("gstin", 1)], name="gstin_idx")
        self.two_b_data.create_index([("dt", 1)], name="date_idx")
        print("Ensured MongoDB indexes are in place for performance.")
        
    def run(self):
        """
        Main execution method. Fetches unprocessed records and reconciles them
        using a thread pool.
        """
        query = {"match_status": {"$exists": False}}
        records_to_process = list(self.parsed_data.find(query))
        total_count = len(records_to_process)

        if total_count == 0:
            print("No new records to process.")
            return

        print(f"Found {total_count} records to reconcile. Starting processing with {MAX_WORKERS} workers...")
        
        update_operations = []
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Each thread needs its own MongoClient instance
            futures = {
                executor.submit(self.process_record, record): record["_id"]
                for record in records_to_process
            }
            
            progress = tqdm(as_completed(futures), total=total_count, desc="Reconciling", unit="record")
            
            for future in progress:
                try:
                    result = future.result()
                    if result:
                        update_operations.append(result)
                except Exception as e:
                    record_id = futures[future]
                    error_op = UpdateOne(
                        {"_id": record_id},
                        {"$set": {
                            "match_status": "error",
                            "error_message": f"Critical failure: {e}"
                        }}
                    )
                    update_operations.append(error_op)

        if update_operations:
            print(f"\nWriting {len(update_operations)} updates to the database...")
            self.parsed_data.bulk_write(update_operations)
            print("Database updates complete.")

        self.generate_summary_report()
        self.client.close()

    def process_record(self, record: Dict[str, Any]) -> Optional[UpdateOne]:
        """
        Processes a single expense record to find the best match in 2B data.
        This function is executed by each worker thread.
        """
        # 1. Clean and prepare input data from the parsed record
        hotel_gstin = clean_gstin(record.get("hotel_gstin"))
        guest_gstin = clean_gstin(record.get("guest_gstin"))
        invoice_num = clean_invoice_number(record.get("invoice_number"))
        checkout_date = parse_date(record.get("checkout_date"))
        inv_amount = record.get("invoice_amount")

        if not checkout_date:
            return UpdateOne({"_id": record["_id"]}, {"$set": {"match_status": "error", "error_message": "Invalid checkout_date"}})
            
        # 2. Define search strategies
        # (parsed_gstin_to_check, 2b_field_to_check, gstin_type)
        strategies = []
        if hotel_gstin:
            strategies.append((hotel_gstin, "ctin", "hotel_gstin_vs_ctin")) # Primary
            strategies.append((hotel_gstin, "gstin", "hotel_gstin_vs_gstin_swapped")) # Swapped
        if guest_gstin:
            strategies.append((guest_gstin, "gstin", "guest_gstin_vs_gstin")) # Primary
            strategies.append((guest_gstin, "ctin", "guest_gstin_vs_ctin_swapped")) # Swapped

        # 3. Execute search and scoring
        best_match = None
        
        # Strategy A: Targeted search using exact GSTINs
        candidates = self._find_candidates_by_gstin(strategies)
        
        if candidates:
            scored_candidates = [
                self._score_candidate(c, hotel_gstin, guest_gstin, invoice_num, checkout_date, inv_amount)
                for c in candidates
            ]
            # Get the candidate with the highest score
            best_match = max(scored_candidates, key=lambda x: x["confidence_score"])

        # Strategy B: If no high-confidence match, try a broader search by proximity
        if not best_match or best_match["confidence_score"] < CONFIRMED_MATCH_THRESHOLD:
            proximity_candidates = self._find_candidates_by_proximity(checkout_date, inv_amount)
            if proximity_candidates:
                scored_prox_candidates = [
                    self._score_candidate(c, hotel_gstin, guest_gstin, invoice_num, checkout_date, inv_amount, is_proximity_search=True)
                    for c in proximity_candidates
                ]
                if scored_prox_candidates:
                    best_proximity_match = max(scored_prox_candidates, key=lambda x: x["confidence_score"])
                    # If this new match is better than what we found before (if any)
                    if not best_match or best_proximity_match["confidence_score"] > best_match["confidence_score"]:
                        best_match = best_proximity_match
        
        # 4. Prepare the update operation for MongoDB
        return self._build_update_operation(record["_id"], best_match)

    def _find_candidates_by_gstin(self, strategies: List[Tuple[str, str, str]]) -> List[Dict[str, Any]]:
        """Finds 2B candidates by querying exact GSTINs based on strategies."""
        candidate_ids = set()
        all_candidates = []
        
        # Re-establish client for this thread
        with MongoClient(MONGO_URI) as thread_client:
            two_b = thread_client[DB_NAME]["two_b"]
            for gstin, field, strategy_name in strategies:
                # Find all docs for this GSTIN
                docs = list(two_b.find({field: gstin}))
                for doc in docs:
                    doc_id = doc["_id"]
                    if doc_id not in candidate_ids:
                        doc["match_strategy_used"] = strategy_name # Tag the doc
                        all_candidates.append(doc)
                        candidate_ids.add(doc_id)
        return all_candidates

    def _find_candidates_by_proximity(self, date: datetime, amount: Optional[float]) -> List[Dict[str, Any]]:
        """Finds candidates in a broader window of date and amount."""
        if not date or not isinstance(amount, (int, float)):
            return []
            
        query = {
            "dt": {
                "$gte": date - timedelta(days=DATE_PROXIMITY_DAYS),
                "$lte": date + timedelta(days=DATE_PROXIMITY_DAYS)
            },
            "val": {
                "$gte": amount * (1 - AMOUNT_PROXIMITY_PCT),
                "$lte": amount * (1 + AMOUNT_PROXIMITY_PCT)
            }
        }
        with MongoClient(MONGO_URI) as thread_client:
            two_b = thread_client[DB_NAME]["two_b"]
            candidates = list(two_b.find(query).limit(20)) # Limit to avoid huge pulls
        
        for cand in candidates:
            cand["match_strategy_used"] = "proximity_date_amount"
        return candidates

    def _score_candidate(self, candidate: Dict, hotel_gstin: str, guest_gstin: str,
                         invoice_num: str, checkout_date: datetime, inv_amount: Optional[float],
                         is_proximity_search: bool = False) -> Dict[str, Any]:
        """Calculates a confidence score for a single 2B candidate."""
        
        # --- Invoice Score ---
        cand_inv = clean_invoice_number(candidate.get("inum"))
        inv_score = 0
        if invoice_num and cand_inv:
            if invoice_num == cand_inv:
                inv_score = 100
            # Containment is a strong signal
            elif invoice_num in cand_inv or cand_inv in invoice_num:
                inv_score = 98
            else:
                inv_score = fuzz.ratio(invoice_num, cand_inv)
        
        # --- Date Score ---
        cand_date = parse_date(candidate.get("dt"))
        date_score = 0
        date_diff = None
        if cand_date:
            date_diff = abs((checkout_date - cand_date).days)
            # Score decays as difference increases
            date_score = max(0, 100 - (date_diff * (100 / DATE_PROXIMITY_DAYS)))
        
        # --- Amount Score ---
        cand_amt = candidate.get("val")
        amt_score = 0
        amt_diff = None
        if isinstance(inv_amount, (int, float)) and isinstance(cand_amt, (int, float)) and inv_amount > 0:
            amt_diff = abs(inv_amount - cand_amt)
            percent_diff = (amt_diff / inv_amount) * 100
            amt_score = max(0, 100 - (percent_diff * (100 / (AMOUNT_PROXIMITY_PCT * 100))))
        
        # --- GSTIN Score ---
        # This is the most complex part
        cand_ctin = clean_gstin(candidate.get("ctin"))
        cand_gstin = clean_gstin(candidate.get("gstin"))
        
        # Score based on fuzzy matching hotel/guest gstin against ctin/gstin
        hotel_vs_ctin = fuzz.ratio(hotel_gstin, cand_ctin) if hotel_gstin and cand_ctin else 0
        hotel_vs_gstin = fuzz.ratio(hotel_gstin, cand_gstin) if hotel_gstin and cand_gstin else 0
        guest_vs_gstin = fuzz.ratio(guest_gstin, cand_gstin) if guest_gstin and cand_gstin else 0
        guest_vs_ctin = fuzz.ratio(guest_gstin, cand_ctin) if guest_gstin and cand_ctin else 0

        # The best possible gstin alignment gives the score
        gstin_score = max(hotel_vs_ctin, hotel_vs_gstin, guest_vs_gstin, guest_vs_ctin)

        # In a targeted (non-proximity) search, an exact GSTIN match was the entrypoint,
        # so we reward that. The fuzzy score helps break ties.
        if not is_proximity_search:
            gstin_score = max(gstin_score, 99)

        # --- Final Weighted Score ---
        final_score = (
            (gstin_score * WEIGHT_GSTIN) +
            (inv_score * WEIGHT_INVOICE) +
            (date_score * WEIGHT_DATE) +
            (amt_score * WEIGHT_AMOUNT)
        )
        
        # Don't consider matches with very low component scores
        if gstin_score < GSTIN_FUZZY_THRESHOLD or inv_score < INVOICE_FUZZY_THRESHOLD:
             # But if date and amount are perfect, it might be a valid but weird case
            if not (date_score > 95 and amt_score > 95):
                final_score *= 0.7 # Penalize heavily if key identifiers are weak
        
        return {
            "2b_doc_id": candidate["_id"],
            "confidence_score": round(final_score, 2),
            "match_strategy": candidate.get("match_strategy_used", "unknown"),
            "details": {
                "gstin_score": round(gstin_score, 2),
                "invoice_score": round(inv_score, 2),
                "date_score": round(date_score, 2),
                "amount_score": round(amt_score, 2),
                "date_diff_days": date_diff,
                "amount_diff_abs": amt_diff,
                "matched_2b_invoice": candidate.get("inum"),
                "matched_2b_ctin": candidate.get("ctin"),
                "matched_2b_gstin": candidate.get("gstin"),
            }
        }
        
    def _build_update_operation(self, record_id: Any, match_result: Optional[Dict]) -> UpdateOne:
        """Constructs a Pymongo UpdateOne operation based on the match result."""
        if not match_result or match_result["confidence_score"] < 40: # Threshold for no match
            update_doc = {
                "match_status": "unmatched",
                "match_details": {"message": "No suitable match found in 2B data."}
            }
        elif match_result["confidence_score"] >= CONFIRMED_MATCH_THRESHOLD:
            update_doc = {
                "match_status": "confirmed",
                "match_details": match_result
            }
        else:
            update_doc = {
                "match_status": "potential_match",
                "match_details": match_result
            }
            
        return UpdateOne({"_id": record_id}, {"$set": update_doc})

    def generate_summary_report(self):
        """Prints a final report of the reconciliation results."""
        print("\n" + "="*30)
        print("   Reconciliation Summary Report")
        print("="*30)
        
        pipeline = [
            {"$group": {"_id": "$match_status", "count": {"$sum": 1}}}
        ]
        results = self.parsed_data.aggregate(pipeline)
        
        summary = {res["_id"]: res["count"] for res in results}
        
        confirmed = summary.get("confirmed", 0)
        potential = summary.get("potential_match", 0)
        unmatched = summary.get("unmatched", 0)
        error = summary.get("error", 0)
        total = confirmed + potential + unmatched + error
        
        print(f"Total Records Analyzed: {total}")
        print("-" * 30)
        print(f"✅ Confirmed Matches:      {confirmed}")
        print(f"🤔 Potential Matches:      {potential}")
        print(f"❌ Unmatched:              {unmatched}")
        print(f"🔥 Errors:                 {error}")
        print("="*30)

# ==============================================================================
# === SCRIPT ENTRY POINT
# ==============================================================================

if __name__ == "__main__":
    processor = ReconciliationProcessor()
    processor.run()
