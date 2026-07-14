from datetime import datetime, date
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import requests

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "http://localhost:3000",
        "http://127.0.0.1:3000",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# URL Endpoints
main_url = "https://www.nseindia.com"
contract_info = "/api/option-chain-contract-info?symbol=NIFTY"
option_chain = "/api/option-chain-v3?type=Indices&symbol=NIFTY"

# Header
headers = {
        'User-Agent': "Mozilla/5.0 (Linux; Android 15; Pixel 9), AppleWebKit/537.36 (KHTML, like Gecko), Chrome/149.0.0.0 Mobile Safari/537.36",
        'Content-Type': "application/json; charset=UTF-8"
    }

# Create Session
session = requests.Session()

# Global Variables declaration


def initialize_session():
    try:
        response = session.get(main_url, headers=headers, timeout=20)
        response.raise_for_status()
        session.cookies.update(response.cookies)
        return True
    except requests.exceptions.RequestException:
        return False


initialize_session()


def parse_expiry(expiry_str):
    try:
        return datetime.strptime(expiry_str, "%d-%b-%Y").date()
    except ValueError:
        return None


def select_expiry_date(expiry_dates, reference_date=None):
    if not expiry_dates:
        raise ValueError("No expiry dates available")

    reference = reference_date or date.today()
    parsed_expiries = []

    for expiry in expiry_dates:
        parsed = parse_expiry(expiry)
        if parsed is not None:
            parsed_expiries.append((parsed, expiry))

    if not parsed_expiries:
        return expiry_dates[0]

    parsed_expiries.sort(key=lambda item: item[0])

    for expiry_date, expiry_str in parsed_expiries:
        if expiry_date >= reference:
            return expiry_str

    return parsed_expiries[-1][1]


@app.get('/')
def home():
    return {"message": "NIFTY Option Chain API is working"}

@app.get('/get-expiry')
def get_expiry_date():
    try:
        initialize_session()
        url = f"{main_url}{contract_info}"
        response = session.get(url, headers=headers, timeout=20)

        response.raise_for_status()

        data = response.json()
        expiry_dates = data.get('expiryDates', [])
        next_expiry = select_expiry_date(expiry_dates)

        return {
            "expiryDates": expiry_dates,
            "selectedExpiry": next_expiry,
        }
    except requests.exceptions.Timeout:
        return {
            "error": "NSE server took too long to respond"
        }
    except requests.exceptions.HTTPError as e:
        return {
            "error": f"NSE returned {e.response.status_code}"
        }

    except Exception as e:
        return {
            "error": str(e)
        }
    

@app.get('/option-chain')
def get_option_chain():
    total_ce_oi = 0
    total_pe_oi = 0
    ce_oi_change = 0
    pe_oi_change = 0
    try:
        initialize_session()
        expiry_response = get_expiry_date()
        expiry_dates = expiry_response.get('expiryDates', [])
        expiry_date = expiry_response.get('selectedExpiry') or select_expiry_date(expiry_dates)
        url = f"{main_url}{option_chain}&expiry={expiry_date}"
        response = session.get(url=url, headers=headers, timeout=20)
        response.raise_for_status()
        data = response.json()
        for strike in data.get("records", {}).get("data", []):
            total_ce_oi += strike.get("CE", {}).get("openInterest", 0) or 0
            total_pe_oi += strike.get("PE", {}).get("openInterest", 0) or 0
            ce_oi_change += strike.get("CE", {}).get("changeinOpenInterest", 0) or 0
            pe_oi_change += strike.get("PE", {}).get("changeinOpenInterest", 0) or 0

        final_data = {
            "Total CE OI": total_ce_oi,
            "Total PE OI": total_pe_oi,
            "CE OI Change": ce_oi_change,
            "PE OI Change": pe_oi_change,
            "selectedExpiry": expiry_date,
        }

        return {
            "data": final_data
        }

    except requests.exceptions.Timeout:
        return {
            "error": "NSE server took too long to respond"
        }
    except requests.exceptions.HTTPError as e:
        return {
            "error": f"NSE returned {e.response.status_code}"
        }

    except Exception as e:
        return {
            "error": str(e)
        }
