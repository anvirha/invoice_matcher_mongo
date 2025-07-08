#  Advanced GST Invoice Matching & Reconciliation Engine

This project automates invoice reconciliation between internal expense records and GSTR-2B data using a **score-based**, **fuzzy-matching**, and **multi-threaded** approach with MongoDB.

---

##  Features

- Exact and fuzzy matching of Invoice Numbers and GSTINs
- Supports swapped GSTIN comparisons (hotel vs guest vs 2B)
- Date and  amount proximity-based fallback logic
- Weighted scoring system for match confidence
- Automatic classification: `confirmed`, `potential_match`, or `unmatched`
- Fast multithreaded processing for large datasets
- MongoDB-based for scalable, batch-ready processing
- Secure environment variable configuration support

-
