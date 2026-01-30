from init import *
def load_dim_cell_site(path,spark):
    df = spark.read.option("multiLine", "true").json(path)
    write_to_iceberg(df, "dim_cell_site")
    print("dim_cell_site loaded")

def load_dim_user(path,spark):
    df = spark.read.option("multiLine", "true").json(path)
    write_to_iceberg(df, "dim_user")
    print("dim_user loaded")

def load_dim_device(path,spark):
    df = spark.read.option("multiLine", "true").json(path)
    write_to_iceberg(df, "dim_device")
    print("dim_device loaded")

def load_dim_agent(path,spark):
    df = spark.read.option("multiLine", "true").json(path)
    write_to_iceberg(df, "dim_agent")
    print("dim_agent loaded")

if __name__ == "__main__":
    spark = get_spark_session()
    load_dim_cell_site("/opt/spark/apps/Dims/dim_cell_site.json",spark)
    load_dim_user("/opt/spark/apps/Dims/DIM_USER.json",spark)
    load_dim_device("/opt/spark/apps/Dims/DIM_DEVICE.json",spark)
    load_dim_agent("/opt/spark/apps/Dims/dim_agent.json",spark)

    print("dims loaded")