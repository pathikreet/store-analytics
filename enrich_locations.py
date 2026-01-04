import time
from tinydb import TinyDB, Query
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut

# Mapping of major Indian cities to their states
INDIA_CITIES = {
    "Mumbai": "Maharashtra", "Delhi": "Delhi", "New Delhi": "Delhi", "Bangalore": "Karnataka", "Bengaluru": "Karnataka",
    "Hyderabad": "Telangana", "Ahmedabad": "Gujarat", "Chennai": "Tamil Nadu", "Kolkata": "West Bengal", "Surat": "Gujarat",
    "Pune": "Maharashtra", "Jaipur": "Rajasthan", "Lucknow": "Uttar Pradesh", "Kanpur": "Uttar Pradesh", "Nagpur": "Maharashtra",
    "Indore": "Madhya Pradesh", "Thane": "Maharashtra", "Bhopal": "Madhya Pradesh", "Visakhapatnam": "Andhra Pradesh",
    "Pimpri-Chinchwad": "Maharashtra", "Patna": "Bihar", "Vadodara": "Gujarat", "Ghaziabad": "Uttar Pradesh", "Ludhiana": "Punjab",
    "Agra": "Uttar Pradesh", "Nashik": "Maharashtra", "Faridabad": "Haryana", "Meerut": "Uttar Pradesh", "Rajkot": "Gujarat",
    "Kalyan-Dombivli": "Maharashtra", "Vasai-Virar": "Maharashtra", "Varanasi": "Uttar Pradesh", "Srinagar": "Jammu and Kashmir",
    "Aurangabad": "Maharashtra", "Dhanbad": "Jharkhand", "Amritsar": "Punjab", "Navi Mumbai": "Maharashtra", "Allahabad": "Uttar Pradesh",
    "Prayagraj": "Uttar Pradesh", "Ranchi": "Jharkhand", "Howrah": "West Bengal", "Coimbatore": "Tamil Nadu", "Jabalpur": "Madhya Pradesh",
    "Gwalior": "Madhya Pradesh", "Vijayawada": "Andhra Pradesh", "Jodhpur": "Rajasthan", "Madurai": "Tamil Nadu", "Raipur": "Chhattisgarh",
    "Kota": "Rajasthan", "Guwahati": "Assam", "Chandigarh": "Chandigarh", "Solapur": "Maharashtra", "Hubli-Dharwad": "Karnataka",
    "Gurgaon": "Haryana", "Gurugram": "Haryana", "Aligarh": "Uttar Pradesh", "Jalandhar": "Punjab", "Noida": "Uttar Pradesh",
    "Dehradun": "Uttarakhand", "Mysore": "Karnataka", "Tiruchirappalli": "Tamil Nadu", "Bhubaneswar": "Odisha", "Salem": "Tamil Nadu",
    "Warangal": "Telangana", "Thiruvananthapuram": "Kerala", "Bhiwandi": "Maharashtra", "Saharanpur": "Uttar Pradesh",
    "Guntur": "Andhra Pradesh", "Amravati": "Maharashtra", "Bikaner": "Rajasthan", "Jammu": "Jammu and Kashmir", "Jamshedpur": "Jharkhand",
    "Bhilai": "Chhattisgarh", "Cuttack": "Odisha", "Kochi": "Kerala", "Udaipur": "Rajasthan", "Firozabad": "Uttar Pradesh",
    "Bhavnagar": "Gujarat", "Dehradun": "Uttarakhand", "Durgapur": "West Bengal", "Asansol": "West Bengal", "Nanded": "Maharashtra",
    "Kolhapur": "Maharashtra", "Ajmer": "Rajasthan", "Gulbarga": "Karnataka", "Jamnagar": "Gujarat", "Ujjain": "Madhya Pradesh",
    "Loni": "Uttar Pradesh", "Siliguri": "West Bengal", "Jhansi": "Uttar Pradesh", "Ulhasnagar": "Maharashtra", "Nellore": "Andhra Pradesh",
    "Mangalore": "Karnataka", "Belgaum": "Karnataka", "Malegaon": "Maharashtra", "Gaya": "Bihar", "Jalgaon": "Maharashtra",
    "Davanagere": "Karnataka", "Kozhikode": "Kerala", "Akola": "Maharashtra", "Kurnool": "Andhra Pradesh", "Bokaro": "Jharkhand",
    "Bellary": "Karnataka", "Patiala": "Punjab", "Agartala": "Tripura", "Bhagalpur": "Bihar", "Muzaffarnagar": "Uttar Pradesh",
    "Latur": "Maharashtra", "Dhule": "Maharashtra", "Tirupati": "Andhra Pradesh", "Rohtak": "Haryana", "Korba": "Chhattisgarh",
    "Bhilwara": "Rajasthan", "Berhampur": "Odisha", "Muzaffarpur": "Bihar", "Ahmednagar": "Maharashtra", "Mathura": "Uttar Pradesh",
    "Kollam": "Kerala", "Avadi": "Tamil Nadu", "Kadapa": "Andhra Pradesh", "Sambalpur": "Odisha", "Bilaspur": "Chhattisgarh",
    "Shahjahanpur": "Uttar Pradesh", "Satara": "Maharashtra", "Bijapur": "Karnataka", "Rampur": "Uttar Pradesh", "Shivamogga": "Karnataka",
    "Chandrapur": "Maharashtra", "Junagadh": "Gujarat", "Thrissur": "Kerala", "Alwar": "Rajasthan", "Bardhaman": "West Bengal",
    "Kakinada": "Andhra Pradesh", "Nizamabad": "Telangana", "Parbhani": "Maharashtra", "Tumkur": "Karnataka", "Khammam": "Telangana",
    "Ozhukarai": "Puducherry", "Bihar Sharif": "Bihar", "Panipat": "Haryana", "Darbhanga": "Bihar", "Aizawl": "Mizoram",
    "Dewas": "Madhya Pradesh", "Ichalkaranji": "Maharashtra", "Karnal": "Haryana", "Bathinda": "Punjab", "Jalna": "Maharashtra",
    "Eluru": "Andhra Pradesh", "Barasat": "West Bengal", "Purnia": "Bihar", "Satna": "Madhya Pradesh", "Mau": "Uttar Pradesh",
    "Sonipat": "Haryana", "Farrukhabad": "Uttar Pradesh", "Sagar": "Madhya Pradesh", "Rourkela": "Odisha", "Durg": "Chhattisgarh",
    "Imphal": "Manipur", "Ratlam": "Madhya Pradesh", "Hapur": "Uttar Pradesh", "Arrah": "Bihar", "Karimnagar": "Telangana",
    "Anantapur": "Andhra Pradesh", "Etawah": "Uttar Pradesh", "Ambernath": "Maharashtra", "Bharatpur": "Rajasthan", "Begusarai": "Bihar",
    "Gandhinagar": "Gujarat", "Puducherry": "Puducherry", "Sikar": "Rajasthan", "Rewa": "Madhya Pradesh", "Mirzapur": "Uttar Pradesh",
    "Raichur": "Karnataka", "Pali": "Rajasthan", "Haridwar": "Uttarakhand", "Vijayanagaram": "Andhra Pradesh", "Katihar": "Bihar",
    "Nagarcoil": "Tamil Nadu", "Sri Ganganagar": "Rajasthan", "Thanjavur": "Tamil Nadu", "Bulandshahr": "Uttar Pradesh",
    "Uluberia": "West Bengal", "Murwara": "Madhya Pradesh", "Sambhal": "Uttar Pradesh", "Singrauli": "Madhya Pradesh",
    "Nadiad": "Gujarat", "Secunderabad": "Telangana", "Yamunanagar": "Haryana", "Bidar": "Karnataka", "Munger": "Bihar",
    "Panchkula": "Haryana", "Burhanpur": "Madhya Pradesh", "Kharagpur": "West Bengal", "Dindigul": "Tamil Nadu", "Gandhidham": "Gujarat",
    "Hospet": "Karnataka", "Malda": "West Bengal", "Ongole": "Andhra Pradesh", "Deoghar": "Jharkhand", "Chapra": "Bihar",
    "Haldia": "West Bengal", "Khandwa": "Madhya Pradesh", "Nandyal": "Andhra Pradesh", "Chittoor": "Andhra Pradesh",
    "Morena": "Madhya Pradesh", "Amroha": "Uttar Pradesh", "Anand": "Gujarat", "Bhind": "Madhya Pradesh", "Bhiwani": "Haryana",
    "Bahraich": "Uttar Pradesh", "Fatehpur": "Uttar Pradesh", "Rae Bareli": "Uttar Pradesh", "Orai": "Uttar Pradesh",
    "Vellore": "Tamil Nadu", "Mahesana": "Gujarat", "Raiganj": "West Bengal", "Sirsa": "Haryana", "Danapur": "Bihar",
    "Serampore": "West Bengal", "Sultanpur": "Uttar Pradesh", "Rishra": "West Bengal", "Haflong": "Assam", "Kalimpong": "West Bengal"
}

def extract_from_name(store_name):
    if not store_name: return None, None
    
    # Sort keys by length descending to match "New Delhi" before "Delhi"
    # Caching this sort would be better for perf, but dictionary is small enough (300 items)
    sorted_keys = sorted(INDIA_CITIES.keys(), key=len, reverse=True)
    
    name_lower = store_name.lower()
    for city in sorted_keys:
        if name_lower.startswith(city.lower()):
            return city, INDIA_CITIES[city]
    
    return None, None

def fetch_city_state(store_name, geolocator):
    """
    Fetches city and state from store name using Heuristic first, then Nominatim.
    """
    if not store_name:
        return "Unknown", "Unknown"
    
    # 1. Heuristic Check
    city, state = extract_from_name(store_name)
    if city and state:
        return city, state
        
    # 2. Geopy Fallback
    try:
        location = geolocator.geocode(store_name, addressdetails=True, timeout=10)
        
        # Fallback cleaning if direct search fails
        if not location:
             clean_name = ''.join([i for i in store_name if not i.isdigit()]).strip()
             if clean_name != store_name:
                 location = geolocator.geocode(clean_name, addressdetails=True, timeout=10)
        
        if location:
            address = location.raw.get('address', {})
            city = address.get('city') or address.get('town') or address.get('village') or address.get('county') or "Unknown"
            state = address.get('state', "Unknown")
            return city, state
    except Exception as e:
        print(f"Error geocoding '{store_name}': {e}")
        
    return "Unknown", "Unknown"

def main():
    db = TinyDB('stores_db.json')
    Store = Query()
    geolocator = Nominatim(user_agent="tumbledry_enricher_offline")
    
    records = db.all()
    print(f"Found {len(records)} records. Starting enrichment...")
    
    updated_count = 0
    
    for record in records:
        store_code = record.get('store_code')
        store_name = record.get('store_name', '')
        yearly_data = record.get('yearly_data', [])
        
        # --- 1. Determine Status ---
        has_data = len(yearly_data) > 0
        info_missing = (store_name == "Not found" or not store_name)
        
        if info_missing:
             status = "Closed" if has_data else "Inactive"
        else:
             status = "Active"
        
        # --- 2. Determine Location ---  
        # Only enrich location if Active and location is missing/Unknown
        current_city = record.get('city')
        current_state = record.get('state')
        
        needs_location = status == "Active" and (not current_city or current_city == "Unknown")
        
        updates = {}
        # Update status if changed
        if record.get('status') != status:
             updates['status'] = status
        
        if needs_location:
            print(f"Enriching {store_code} ({store_name})...")
            city, state = fetch_city_state(store_name, geolocator)
            updates['city'] = city
            updates['state'] = state
            # Rate limit for Nominatim (1 sec is polite)
            time.sleep(1.1) 
        elif status in ["Inactive", "Closed"]:
             # Ensure inactive/closed stores maintain consistent schema
             if not current_city: updates['city'] = "Unknown"
             if not current_state: updates['state'] = "Unknown"
        
        if updates:
            db.update(updates, Store.store_code == store_code)
            updated_count += 1
            print(f"Updated {store_code}: {updates}")
    
    print(f"\nEnrichment complete. Updated {updated_count} records.")

if __name__ == "__main__":
    main()
