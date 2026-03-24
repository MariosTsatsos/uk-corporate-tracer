import sqlite3
from config import DB_PATH


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    conn = get_conn()
    c = conn.cursor()

    c.execute("""
        CREATE TABLE IF NOT EXISTS director_companies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            director_name TEXT,
            director_dob TEXT,
            company_number TEXT,
            company_name TEXT,
            role TEXT,
            appointed_on TEXT,
            resigned_on TEXT,
            officer_id TEXT,
            search_name TEXT,
            UNIQUE(director_name, company_number, role)
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS companies (
            company_number TEXT PRIMARY KEY,
            company_name TEXT,
            status TEXT,
            company_type TEXT,
            incorporated_on TEXT,
            registered_office TEXT,
            sic_codes TEXT,
            fetched_at TEXT
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS psc (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_number TEXT,
            psc_name TEXT,
            psc_kind TEXT,
            natures_of_control TEXT,
            notified_on TEXT,
            ceased_on TEXT,
            dob_year INTEGER,
            dob_month INTEGER,
            nationality TEXT,
            country_of_residence TEXT,
            address TEXT,
            registration_number TEXT,
            UNIQUE(company_number, psc_name, notified_on)
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS addresses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            address_raw TEXT,
            source_type TEXT,
            source_id TEXT,
            source_name TEXT,
            UNIQUE(address_raw, source_type, source_id)
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS company_officers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_number TEXT,
            officer_name TEXT,
            officer_role TEXT,
            appointed_on TEXT,
            resigned_on TEXT,
            dob_year INTEGER,
            dob_month INTEGER,
            nationality TEXT,
            country_of_residence TEXT,
            address TEXT,
            officer_id TEXT,
            UNIQUE(company_number, officer_name, officer_role)
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS charges (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_number TEXT,
            charge_code TEXT,
            charge_holder TEXT,
            charge_created TEXT,
            charge_status TEXT,
            charge_description TEXT,
            assets_ceased_released TEXT,
            charge_document_link TEXT,
            UNIQUE(company_number, charge_code)
        )
    """)

    # Full LR property record — all useful CCOD/OCOD/LEASES fields stored
    c.execute("""
        CREATE TABLE IF NOT EXISTS properties (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title_number TEXT,
            tenure TEXT,
            property_address TEXT,
            district TEXT,
            county TEXT,
            region TEXT,
            postcode TEXT,
            multiple_address_indicator TEXT,
            price_paid TEXT,
            date_proprietor_added TEXT,
            owner_company TEXT,
            owner_company_number TEXT,
            proprietorship_category TEXT,
            proprietor_address TEXT,
            dataset_source TEXT,
            UNIQUE(title_number, owner_company_number)
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS personal_properties (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title_number TEXT,
            tenure TEXT,
            property_address TEXT,
            district TEXT,
            county TEXT,
            region TEXT,
            postcode TEXT,
            price_paid TEXT,
            date_proprietor_added TEXT,
            owner_name TEXT,
            proprietor_address TEXT,
            dataset_source TEXT,
            UNIQUE(title_number, owner_name)
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS overseas_entities (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            entity_name TEXT,
            beneficial_owner TEXT,
            property_title TEXT,
            country TEXT,
            UNIQUE(entity_name, property_title)
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS fetch_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fetch_type TEXT,
            fetch_key TEXT,
            fetched_at TEXT,
            result_count INTEGER,
            UNIQUE(fetch_type, fetch_key)
        )
    """)

    conn.commit()
    conn.close()
    print("[DB] Initialised.")


if __name__ == "__main__":
    init_db()
