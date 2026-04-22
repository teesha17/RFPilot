from app.db.connection import get_db

def get_catalog(company_id):
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
    SELECT sku_id, sku_code, product_name, voltage_kv,
           conductor_material, cross_section_mm2,
           insulation_type, armoured, number_of_cores,
           price_per_meter, stock_meters
    FROM product_catalog
    WHERE company_id=%s AND is_active=TRUE
    """, (company_id,))

    return cur.fetchall()