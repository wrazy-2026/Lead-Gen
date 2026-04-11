import requests
import logging
import json
import time

logger = logging.getLogger(__name__)

class GHLService:
    def __init__(self, api_key: str, location_id: str = None):
        self.api_key = api_key
        self.location_id = location_id
        # Use V2 if location_id is provided, else fallback to V1 endpoints if possible
        # Actually, most users still use V1 for simple API key access, or V2 with PAT
        self.base_url = "https://services.leadconnectorhq.com/contacts/" if location_id else "https://rest.gohighlevel.com/v1/contacts/"
        
    def create_contact(self, lead_data: dict, tag: str = None):
        """Create a contact in GHL."""
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        # Map our lead fields to GHL fields
        # GHL V1/V2 standard fields: firstName, lastName, name, email, phone, companyName, address1, city, state, country, postalCode, website, tags
        
        name_parts = lead_data.get('business_name', '').split(' ', 1)
        first_name = lead_data.get('first_name') or name_parts[0]
        last_name = lead_data.get('last_name') or (name_parts[1] if len(name_parts) > 1 else '')
        
        payload = {
            "firstName": first_name,
            "lastName": last_name,
            "name": lead_data.get('business_name'),
            "companyName": lead_data.get('business_name'),
            "email": lead_data.get('email_1') or lead_data.get('email'),
            "phone": lead_data.get('phone_1') or lead_data.get('phone') or lead_data.get('business_phone'),
            "address1": lead_data.get('address') or lead_data.get('business_address'),
            "city": lead_data.get('city'),
            "state": lead_data.get('state'),
            "website": lead_data.get('website') or lead_data.get('domain'),
            "tags": [tag] if tag else ["lead_scraper"]
        }
        
        if self.location_id:
            payload["locationId"] = self.location_id
            # V2 payload might slightly differ, but usually it's compatible for basic fields
            url = "https://services.leadconnectorhq.com/contacts/"
        else:
            url = "https://rest.gohighlevel.com/v1/contacts/"
            
        try:
            # GHL doesn't like empty fields sometimes, clean up
            payload = {k: v for k, v in payload.items() if v}
            
            response = requests.post(url, headers=headers, json=payload, timeout=10)
            if response.status_code in [200, 201]:
                return True, response.json()
            else:
                logger.error(f"GHL Error ({response.status_code}): {response.text}")
                return False, response.text
        except Exception as e:
            logger.error(f"GHL Exception: {str(e)}")
            return False, str(e)

    def export_leads(self, leads: list, tag: str = None):
        """Export a list of leads to GHL."""
        success_count = 0
        failed_count = 0
        errors = []
        
        for lead in leads:
            # Avoid hitting rate limits (GHL is usually 100 reqs/min for V1)
            time.sleep(0.5) 
            success, result = self.create_contact(lead, tag)
            if success:
                success_count += 1
            else:
                failed_count += 1
                errors.append({"lead": lead.get('business_name'), "error": result})
                
        return {
            "success": success_count,
            "failed": failed_count,
            "errors": errors[:5] # Return first 5 errors
        }
