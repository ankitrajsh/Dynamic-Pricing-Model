#!/usr/bin/env python3
"""
Dynamic Pricing CSV Data Analysis Script
Analyzes the CSV database files for pricing insights and recommendations
"""

import pandas as pd
import os
from datetime import datetime, timedelta

# Directory containing CSV files
CSV_DIR = 'csv_data'

def load_data():
    """Load all CSV files into pandas DataFrames"""
    data = {}
    tables = [
        'categories', 'products', 'customers', 'competitors',
        'inventory', 'pricing_history', 'competitor_prices',
        'demand_metrics', 'pricing_rules', 'orders',
        'order_items', 'price_adjustments'
    ]
    
    for table in tables:
        file_path = os.path.join(CSV_DIR, f'{table}.csv')
        if os.path.exists(file_path):
            data[table] = pd.read_csv(file_path)
            print(f"✓ Loaded {table}: {len(data[table])} records")
        else:
            print(f"✗ File not found: {file_path}")
    
    return data

def product_pricing_overview(data):
    """Generate product pricing overview"""
    print("\n" + "="*80)
    print("PRODUCT PRICING OVERVIEW")
    print("="*80)
    
    products = data['products']
    inventory = data['inventory']
    demand = data['demand_metrics']
    
    # Get latest demand metrics
    demand['date'] = pd.to_datetime(demand['date'])
    latest_date = demand['date'].max()
    latest_demand = demand[demand['date'] == latest_date]
    
    # Merge data
    overview = products.merge(inventory, on='product_id', how='left')
    overview = overview.merge(
        latest_demand[['product_id', 'demand_score', 'conversion_rate', 'page_views']], 
        on='product_id', 
        how='left'
    )
    
    # Calculate price change from base
    overview['price_change_pct'] = ((overview['current_price'] - overview['base_price']) / 
                                     overview['base_price'] * 100).round(2)
    
    # Display
    display_cols = [
        'product_name', 'current_price', 'quantity_available', 
        'stock_status', 'demand_score', 'conversion_rate', 'price_change_pct'
    ]
    print(overview[display_cols].to_string(index=False))

def competitor_comparison(data):
    """Compare our prices with competitors"""
    print("\n" + "="*80)
    print("COMPETITOR PRICE COMPARISON")
    print("="*80)
    
    products = data['products']
    comp_prices = data['competitor_prices']
    competitors = data['competitors']
    
    # Calculate average competitor prices
    avg_comp = comp_prices.groupby('product_id')['competitor_price'].agg([
        ('avg_competitor_price', 'mean'),
        ('min_competitor_price', 'min'),
        ('max_competitor_price', 'max'),
        ('competitor_count', 'count')
    ]).reset_index()
    
    # Merge with products
    comparison = products.merge(avg_comp, on='product_id', how='left')
    
    # Calculate price difference
    comparison['price_gap'] = (comparison['current_price'] - 
                                comparison['avg_competitor_price']).round(2)
    comparison['price_gap_pct'] = ((comparison['price_gap'] / 
                                     comparison['avg_competitor_price']) * 100).round(2)
    comparison['position'] = comparison['price_gap_pct'].apply(
        lambda x: 'Lower' if x < 0 else 'Higher' if x > 0 else 'Equal'
    )
    
    # Display
    display_cols = [
        'product_name', 'current_price', 'avg_competitor_price', 
        'min_competitor_price', 'price_gap_pct', 'position'
    ]
    print(comparison[display_cols].to_string(index=False))

def high_demand_products(data):
    """Identify high demand products"""
    print("\n" + "="*80)
    print("HIGH DEMAND PRODUCTS (Potential for Price Increase)")
    print("="*80)
    
    products = data['products']
    demand = data['demand_metrics']
    inventory = data['inventory']
    
    # Get recent demand (last 3 days average)
    demand['date'] = pd.to_datetime(demand['date'])
    recent_date = demand['date'].max() - timedelta(days=2)
    recent_demand = demand[demand['date'] >= recent_date].groupby('product_id').agg({
        'demand_score': 'mean',
        'conversion_rate': 'mean',
        'revenue': 'sum',
        'page_views': 'sum'
    }).reset_index()
    
    # Merge data
    analysis = products.merge(recent_demand, on='product_id', how='left')
    analysis = analysis.merge(inventory[['product_id', 'quantity_available']], 
                              on='product_id', how='left')
    
    # Filter high demand
    high_demand = analysis[analysis['demand_score'] > 7.5].copy()
    high_demand['price_increase_potential'] = (high_demand['max_price'] - 
                                                high_demand['current_price']).round(2)
    high_demand['recommended_price'] = (high_demand['current_price'] * 1.05).round(2)
    high_demand['recommended_price'] = high_demand[['recommended_price', 'max_price']].min(axis=1)
    
    # Sort by demand score
    high_demand = high_demand.sort_values('demand_score', ascending=False)
    
    # Display
    display_cols = [
        'product_name', 'current_price', 'recommended_price', 
        'quantity_available', 'demand_score', 'conversion_rate'
    ]
    print(high_demand[display_cols].to_string(index=False))

def revenue_analysis(data):
    """Analyze revenue by product"""
    print("\n" + "="*80)
    print("REVENUE ANALYSIS")
    print("="*80)
    
    products = data['products']
    order_items = data['order_items']
    
    # Calculate revenue and profit
    revenue = order_items.groupby('product_id').agg({
        'quantity': 'sum',
        'total_price': 'sum',
        'order_id': 'count'
    }).reset_index()
    revenue.columns = ['product_id', 'units_sold', 'total_revenue', 'order_count']
    
    # Merge with products
    analysis = products.merge(revenue, on='product_id', how='left').fillna(0)
    
    # Calculate profit
    analysis['total_cost'] = analysis['base_cost'] * analysis['units_sold']
    analysis['gross_profit'] = analysis['total_revenue'] - analysis['total_cost']
    analysis['profit_margin_pct'] = (
        (analysis['gross_profit'] / analysis['total_revenue']) * 100
    ).round(2)
    
    # Sort by revenue
    analysis = analysis.sort_values('total_revenue', ascending=False)
    
    # Display
    display_cols = [
        'product_name', 'units_sold', 'total_revenue', 
        'gross_profit', 'profit_margin_pct'
    ]
    print(analysis[display_cols].to_string(index=False))
    
    # Summary
    print(f"\nTotal Revenue: ${analysis['total_revenue'].sum():.2f}")
    print(f"Total Profit: ${analysis['gross_profit'].sum():.2f}")
    print(f"Average Margin: {analysis['profit_margin_pct'].mean():.2f}%")

def inventory_alerts(data):
    """Identify inventory issues"""
    print("\n" + "="*80)
    print("INVENTORY ALERTS")
    print("="*80)
    
    products = data['products']
    inventory = data['inventory']
    demand = data['demand_metrics']
    
    # Merge data
    inv_analysis = products.merge(inventory, on='product_id', how='left')
    
    # Get recent sales velocity
    demand['date'] = pd.to_datetime(demand['date'])
    recent_demand = demand[demand['date'] >= demand['date'].max() - timedelta(days=7)]
    velocity = recent_demand.groupby('product_id')['purchase_count'].mean().reset_index()
    velocity.columns = ['product_id', 'avg_daily_sales']
    
    inv_analysis = inv_analysis.merge(velocity, on='product_id', how='left').fillna(0)
    
    # Calculate days of stock
    inv_analysis['days_of_stock'] = (
        inv_analysis['quantity_available'] / inv_analysis['avg_daily_sales']
    ).replace([float('inf'), float('-inf')], 999).fillna(999).round(1)
    
    # Identify issues
    print("\n🔴 LOW STOCK (Below Reorder Point):")
    low_stock = inv_analysis[
        inv_analysis['quantity_available'] <= inv_analysis['reorder_point']
    ]
    if len(low_stock) > 0:
        print(low_stock[['product_name', 'quantity_available', 'reorder_point', 
                         'days_of_stock']].to_string(index=False))
    else:
        print("None")
    
    print("\n🟡 SLOW MOVING (>30 days of stock):")
    slow_moving = inv_analysis[inv_analysis['days_of_stock'] > 30]
    if len(slow_moving) > 0:
        print(slow_moving[['product_name', 'quantity_available', 'current_price', 
                           'days_of_stock']].to_string(index=False))
    else:
        print("None")

def pricing_recommendations(data):
    """Generate dynamic pricing recommendations"""
    print("\n" + "="*80)
    print("DYNAMIC PRICING RECOMMENDATIONS")
    print("="*80)
    
    products = data['products']
    inventory = data['inventory']
    demand = data['demand_metrics']
    comp_prices = data['competitor_prices']
    
    # Get latest demand
    demand['date'] = pd.to_datetime(demand['date'])
    latest_demand = demand[demand['date'] == demand['date'].max()]
    
    # Average competitor prices
    avg_comp = comp_prices.groupby('product_id')['competitor_price'].mean().reset_index()
    avg_comp.columns = ['product_id', 'avg_competitor_price']
    
    # Merge all data
    reco = products.merge(inventory[['product_id', 'quantity_available', 'stock_status']], 
                          on='product_id', how='left')
    reco = reco.merge(latest_demand[['product_id', 'demand_score', 'conversion_rate']], 
                      on='product_id', how='left')
    reco = reco.merge(avg_comp, on='product_id', how='left')
    
    recommendations = []
    
    for _, row in reco.iterrows():
        action = None
        reason = None
        recommended_price = row['current_price']
        
        # High demand + good stock = Increase price
        if row['demand_score'] > 7.5 and row['quantity_available'] > 50:
            recommended_price = min(row['current_price'] * 1.05, row['max_price'])
            action = "INCREASE"
            reason = f"High demand ({row['demand_score']:.1f}) with adequate stock"
        
        # Low demand + high stock = Decrease price
        elif row['demand_score'] < 5 and row['quantity_available'] > 100:
            recommended_price = max(row['current_price'] * 0.92, row['min_price'])
            action = "DECREASE"
            reason = f"Low demand ({row['demand_score']:.1f}) with excess inventory"
        
        # Above competitor average = Match competitors
        elif pd.notna(row['avg_competitor_price']) and row['current_price'] > row['avg_competitor_price'] * 1.05:
            recommended_price = max(row['avg_competitor_price'] * 0.98, row['min_price'])
            action = "DECREASE"
            reason = f"Price above competitors (${row['avg_competitor_price']:.2f})"
        
        # Low stock + high demand = Increase more
        elif row['stock_status'] == 'low_stock' and row['demand_score'] > 6:
            recommended_price = min(row['current_price'] * 1.08, row['max_price'])
            action = "INCREASE"
            reason = "Low stock with sustained demand"
        
        if action:
            recommendations.append({
                'product_name': row['product_name'],
                'current_price': row['current_price'],
                'recommended_price': round(recommended_price, 2),
                'action': action,
                'change_pct': round(((recommended_price - row['current_price']) / 
                                     row['current_price'] * 100), 2),
                'reason': reason
            })
    
    if recommendations:
        reco_df = pd.DataFrame(recommendations)
        reco_df = reco_df.sort_values('change_pct', ascending=False)
        print(reco_df.to_string(index=False))
    else:
        print("No pricing changes recommended at this time.")

def main():
    """Main analysis function"""
    print("\n" + "="*80)
    print("DYNAMIC PRICING DATABASE ANALYSIS")
    print("="*80)
    print(f"Analysis Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80)
    
    # Load data
    print("\nLoading CSV data...")
    data = load_data()
    
    if not data:
        print("\n✗ Error: No data loaded. Please ensure CSV files are in the 'csv_data' directory.")
        return
    
    # Run analyses
    try:
        product_pricing_overview(data)
        competitor_comparison(data)
        high_demand_products(data)
        revenue_analysis(data)
        inventory_alerts(data)
        pricing_recommendations(data)
        
        print("\n" + "="*80)
        print("ANALYSIS COMPLETE")
        print("="*80)
        
    except Exception as e:
        print(f"\n✗ Error during analysis: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
