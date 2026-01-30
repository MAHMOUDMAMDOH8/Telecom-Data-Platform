from init import *

def show_table_schema(table_name, spark):
    """Display the schema for a given Iceberg table."""
    full_table = f"local.{table_name}"
    
    print(f"\n{'='*80}")
    print(f"Schema for table: {full_table}")
    print(f"{'='*80}")
    
    try:
        if not spark.catalog.tableExists(full_table):
            print(f"❌ Table {full_table} does not exist.")
            return
        
        # Get the table and show schema
        df = spark.table(full_table)
        
        print(f"\n📊 Column Schema:")
        print("-" * 80)
        df.printSchema()
        
        # Show some statistics
        row_count = df.count()
        print(f"\n📈 Row Count: {row_count:,}")
        
        # Show column names and types in a formatted way
        print(f"\n📋 Column Details:")
        print("-" * 80)
        schema = df.schema
        for field in schema.fields:
            nullable = "NULL" if field.nullable else "NOT NULL"
            print(f"  • {field.name:40} | {str(field.dataType):30} | {nullable}")
        
        # Show sample data
        print(f"\n🔍 Sample Data (first 5 rows):")
        print("-" * 80)
        df.show(5, truncate=False)
        
    except Exception as e:
        print(f"❌ Error reading table {full_table}: {e}")
        import traceback
        traceback.print_exc()


def show_all_schemas(table_names, spark):
    """Display schemas for multiple tables."""
    print("\n" + "="*80)
    print("ICEBERG TABLE SCHEMAS")
    print("="*80)
    
    for table_name in table_names:
        show_table_schema(table_name, spark)
        print("\n")


if __name__ == "__main__":
    # Initialize Spark session
    spark = get_spark_session(app_name="Show-Iceberg-Schemas")
    
    # Tables to show schemas for
    tables = ['support', 'recharge', 'payment', 'sms', 'calls']
    
    # Show schemas for all tables
    show_all_schemas(tables, spark)
    
    print("\n" + "="*80)
    print("✅ Schema inspection complete!")
    print("="*80)
    
    spark.stop()

