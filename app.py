from flask import Flask, render_template, request, jsonify
from tinydb import TinyDB, Query
import pandas as pd
import os

app = Flask(__name__)
db_path = 'stores_db.json'
db = TinyDB(db_path)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/analytics/<store_code>')
def analytics(store_code):
    return render_template('analytics.html', store_code=store_code)

@app.route('/api/stores')
def api_stores():
    # Pagination & Filtering
    page = int(request.args.get('page', 1))
    limit = int(request.args.get('limit', 10))
    search = request.args.get('search', '').lower()
    
    # Column Filters
    f_code = request.args.get('code', '').lower()
    f_name = request.args.get('name', '').lower()
    f_city = request.args.get('city', '').lower()
    f_state = request.args.get('state', '').lower()
    f_status = request.args.get('status', '').lower()
    
    all_records = db.all()
    filtered_records = []
    
    for r in all_records:
        r_code = str(r.get('store_code', '')).lower()
        r_name = str(r.get('store_name', '')).lower()
        r_city = str(r.get('city', 'Unknown')).lower()
        r_state = str(r.get('state', 'Unknown')).lower()
        r_status = str(r.get('status', 'Unknown')).lower()
        r_launch = str(r.get('launch_date', 'Unknown')).lower()

        # Calculate Avg TAT
        yearly_data = r.get('yearly_data', [])
        tat_values = []
        for row in yearly_data:
             val = str(row.get('% Delivered within TAT', '0')).replace('%', '').strip()
             if val and val != '-':
                 try:
                     tat_values.append(float(val))
                 except: pass
        
        avg_tat = sum(tat_values) / len(tat_values) if tat_values else 0

        # Global Search
        if search:
            text_content = f"{r_code} {r_name} {r_city} {r_state} {r_status}".lower()
            if search not in text_content:
                continue
        
        # Column Filters
        if f_code and f_code not in r_code: continue
        if f_name and f_name not in r_name: continue
        if f_city and f_city not in r_city: continue
        if f_state and f_state not in r_state: continue
        if f_status and f_status != 'all' and f_status != r_status: continue

        filtered_records.append({
            'store_code': r.get('store_code'),
            'store_name': r.get('store_name'),
            'city': r.get('city', 'Unknown'),
            'state': r.get('state', 'Unknown'),
            'status': r.get('status', 'Unknown'),
            'launch_date': r.get('launch_date', 'Unknown'),
            'avg_tat': round(avg_tat, 1)
        })
    
    # Sorting
    sort_by = request.args.get('sort_by', '')
    order = request.args.get('order', 'asc')
    
    if sort_by:
        reverse = (order == 'desc')
        def get_sort_val(x):
            val = x.get(sort_by)
            if val is None: return 0 if sort_by in ['avg_tat'] else ''
            if isinstance(val, str): return val.lower()
            return val
            
        filtered_records.sort(key=get_sort_val, reverse=reverse)
        
    total_count = len(filtered_records)
    start = (page - 1) * limit
    end = start + limit
    paginated_data = filtered_records[start:end]
    
    return jsonify({
        'total': total_count,
        'page': page,
        'limit': limit,
        'data': paginated_data
    })

@app.route('/api/stats/<store_code>')
def api_stats(store_code):
    Store = Query()
    record = db.search(Store.store_code == store_code)
    
    if not record:
        return jsonify({'error': 'Store not found'}), 404
        
    data = record[0].get('yearly_data', [])
    
    # Robust Sorting: Parse date and sort chronologically
    # Format expected: "Jan, 2024"
    from datetime import datetime
    
    parsed_data = []
    for row in data:
        month_str = row.get('Month', '')
        try:
            # Parse date for sorting
            dt = datetime.strptime(month_str, "%b, %Y")
        except ValueError:
            # Fallback for unexpected formats, put at end or handle gracefully
            dt = datetime.min
            
        parsed_data.append({
            'dt': dt,
            'Month': month_str,
            'Revenue': row.get('Revenue', 0),
            'Chemical Billing': row.get('Chemical Billing', 0),
            'Packaging Billing': row.get('Packaging Billing', 0),
            'Chemical Pct': row.get('% Chemical Billing Vs Revenue', 0),
            'Packaging Pct': row.get('% Packaging Billing Vs Revenue', 0),
            'TAT Pct': row.get('% Delivered within TAT', 0),
            'Revenue Growth Vs Last Month %': row.get('Revenue Growth Vs Last Month %', 0)
        })
    
    # Sort by datetime object (Earliest to Latest)
    parsed_data.sort(key=lambda x: x['dt'])
    
    months = []
    revenue = []
    chemical = []
    packaging = []
    chemical_pct = []
    packaging_pct = []
    tat_pct = []
    growth = []
    
    for item in parsed_data:
        months.append(item['Month'])
        
        def clean_num(val):
            if not val: return 0
            val = str(val).replace('₹', '').replace(',', '').replace('%', '').strip()
            if val == '-' or val == '': return 0
            return float(val)

        revenue.append(clean_num(item['Revenue']))
        chemical.append(clean_num(item['Chemical Billing']))
        packaging.append(clean_num(item['Packaging Billing']))
        chemical_pct.append(clean_num(item['Chemical Pct']))
        packaging_pct.append(clean_num(item['Packaging Pct']))
        tat_pct.append(clean_num(item['TAT Pct']))
        growth.append(clean_num(item['Revenue Growth Vs Last Month %']))

    return jsonify({
        'store_name': record[0].get('store_name'),
        'labels': months,
        'revenue': revenue,
        'chemical': chemical,
        'packaging': packaging,
        'chemical_pct': chemical_pct,
        'packaging_pct': packaging_pct,
        'tat_pct': tat_pct,
        'growth': growth,
        'kpis': {
            'store_age': calculate_store_age(record[0].get('launch_date', '')),
            'lifetime_revenue': sum(revenue),
            'avg_monthly_revenue': sum(revenue) / len(revenue) if revenue else 0,
            'efficiency_score': (sum(revenue) / (sum(chemical) + sum(packaging))) if (sum(chemical) + sum(packaging)) > 0 else 0,
            'highest_revenue_month': get_highest_month(months, revenue)
        }
    })

def calculate_store_age(launch_date_str):
    if not launch_date_str: return "N/A"
    try:
        from datetime import datetime
        # Launch date format example: "12 Jul 2025"
        launch_date = datetime.strptime(launch_date_str, "%d %b %Y")
        now = datetime.now()
        diff = now - launch_date
        
        years = diff.days // 365
        months = (diff.days % 365) // 30
        
        if years > 0:
            return f"{years} Years, {months} Months"
        else:
            return f"{months} Months"
    except:
        return launch_date_str

def get_highest_month(months, revenue):
    if not revenue: return {"month": "-", "amount": 0}
    max_rev = max(revenue)
    index = revenue.index(max_rev)
    return {"month": months[index], "amount": max_rev}

@app.route('/api/update_locations', methods=['POST'])
def update_locations():
    data = request.json
    updates = data.get('updates', [])
    count = 0
    
    Store = Query()
    for item in updates:
        store_code = item.get('storeCode')
        city = item.get('city')
        state = item.get('state')
        
        if store_code:
            payload = {}
            if city is not None: payload['city'] = city
            if state is not None: payload['state'] = state
            
            if payload:
                db.update(payload, Store.store_code == store_code)
                count += 1
                
    return jsonify({'success': True, 'updated': count})

if __name__ == '__main__':
    app.run(debug=True, port=5000)
