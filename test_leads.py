from app_flask import app
from database import get_database

db = get_database()
print("Initial count:", db.get_leads_count())

with app.test_client() as client:
    response = client.get('/leads')
    print("Status code:", response.status_code)
    html = response.data.decode('utf-8')
    print("Total leads in HTML:", html.count('<tr class="lead-row'))
    if "No leads match your criteria" in html:
        print("Empty table state rendered!")
    else:
        print("Table seems populated.")
