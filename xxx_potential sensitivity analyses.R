
NEED TO SOMEHOW SHOW WHEN DROPPING ALL THE OBSERVATIONS IN THE NA SALE CATEGORY, ITS DROPPING NON ARMS LENGTH TRANSACTIONS

Here's what we've discussed as potential sensitivity analyses:
  Investor identification:
  
  Using investor == 1 as a secondary/alternative corporate investor flag — captures small individual landlords but misses obvious corporate entities, so not the primary definition but useful as a robustness check on the breadth of the investor definition
Using buy_occ == T among non-corporate buyers to identify absentee/individual landlord purchases — useful for checking whether results hold when individual landlords are included alongside corporate investors
Dropping the 1,882 transactions with inconsistent corporate flags across buyer positions vs keeping them

Sample restrictions:
  4. Dropping interfamily == 1 — we just discussed uncertainty about whether this is the right call, so running with and without is a natural sensitivity check
5. Restricting to sale_doc_type == "GD" only (grant deeds) as the cleanest arms-length instrument, vs keeping all doc types
6. Dropping buy_occ == T among non-corporate buyers to exclude potential vacation property purchases from the comparison group
Geography/time:
  7. Using the 2007 FHSZ map vs the 2022 updated map for the wildfire risk classification — affects which parcels are in the high risk category, especially for transactions straddling the 2022 reclassification