import os
import logging
import google.generativeai as genai
from typing import Optional
import json

logger = logging.getLogger(__name__)

class GeminiService:
    def __init__(self, api_key: str = None):
        self.api_key = api_key or os.environ.get('GEMINI_API_KEY')
        if not self.api_key:
            logger.warning("GEMINI_API_KEY not found in environment")
            self.model = None
        else:
            try:
                genai.configure(api_key=self.api_key)
                self.model = genai.GenerativeModel('gemini-2.5-flash')
                logger.info("Gemini Service initialized successfully")
            except Exception as e:
                logger.error(f"Failed to initialize Gemini: {e}")
                self.model = None

    def generate_text(self, prompt: str) -> Optional[str]:
        """
        General-purpose text generation using Gemini.
        
        Args:
            prompt: The prompt to send to Gemini
            
        Returns:
            Generated text string or None on error
        """
        if not self.model:
            return None
        try:
            response = self.model.generate_content(prompt)
            return response.text.strip() if response and response.text else None
        except Exception as e:
            logger.error(f"Error calling Gemini generate_text: {e}")
            return None

    def find_business_domain(self, business_name: str, state: str, address: str = None) -> Optional[str]:
        if not self.model:
            return None
            
        # Clean business name
        clean_name = business_name.replace('"', '').replace("'", "")
        
        prompt = f"""
        Find the official website domain for the following business registered in the US.
        Business Name: {clean_name}
        State: {state}
        {f'Address: {address}' if address else ''}
        
        Return ONLY the domain name (e.g. example.com). 
        If you are certain it does not have a website or you cannot find it, return exactly "NOT FOUND".
        Do not include https:// or any other text.
        """
        
        try:
            response = self.model.generate_content(prompt)
            result = response.text.strip()
            
            if not result or "NOT FOUND" in result.upper():
                return "Not Found"
            
            # Basic cleaning (remove possible markdown or prefixes)
            result = result.replace('`', '').replace('http://', '').replace('https://', '').replace('www.', '').split('/')[0]
            
            if "." in result and len(result) > 3:
                return result.lower()
            return "Not Found"
        except Exception as e:
            logger.error(f"Error calling Gemini for {business_name}: {e}")
            return None

    def classify_business_category(self, business_name: str, state: str = None, address: str = None) -> Optional[str]:
        """Classify the likely business category from available business attributes."""
        if not self.model or not business_name:
            return None

        prompt = f"""
        You classify US businesses into one concise category.
        Business Name: {business_name}
        State: {state or 'Unknown'}
        Address: {address or 'Unknown'}

        Return JSON only in this format:
        {{"category": "...", "confidence": 0.0}}

        Category should be short and practical (examples: Construction, Real Estate, Retail, Food & Beverage,
        Healthcare, Logistics, Technology, Professional Services, Manufacturing, Finance, Legal, Education).
        If uncertain, return category as "Unknown".
        """

        try:
            response = self.model.generate_content(prompt)
            if not response or not response.text:
                return None

            text = response.text.strip()
            if '```' in text:
                parts = text.split('```')
                text = parts[1] if len(parts) > 1 else text
                if text.startswith('json'):
                    text = text[4:]

            parsed = json.loads(text.strip())
            category = (parsed.get('category') or '').strip()
            if category:
                return category
            return None
        except Exception as e:
            logger.error(f"Error classifying business category for {business_name}: {e}")
            return None

_gemini_service = None

def get_gemini_service():
    global _gemini_service
    if _gemini_service is None:
        _gemini_service = GeminiService()
    return _gemini_service
