"""
cleanup_adults.py

One-time script to remove any adult records (age >= 18) already in the database.
Run once after updating the scrapers:  python3 cleanup_adults.py
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from database.models import init_db, MissingPerson
from utils.helpers import is_minor

_, Session = init_db("missing_children.db")
db = Session()

# Records with explicit age >= 18
confirmed_adults = db.query(MissingPerson).filter(
    MissingPerson.age_at_disappearance >= 18
).all()

# Records where DOB tells us they were 18+ at disappearance
from datetime import date
dob_adults = []
for r in db.query(MissingPerson).filter(
    MissingPerson.age_at_disappearance == None,
    MissingPerson.date_of_birth != None,
).all():
    if not is_minor(None, r.date_of_birth, r.date_missing):
        dob_adults.append(r)

to_delete = confirmed_adults + dob_adults
print(f"Found {len(confirmed_adults)} records with age >= 18")
print(f"Found {len(dob_adults)} records where DOB indicates adult")
print(f"Total to remove: {len(to_delete)}")

if to_delete:
    for r in to_delete:
        db.delete(r)
    db.commit()
    print("Deleted successfully.")
else:
    print("Nothing to delete.")

db.close()
