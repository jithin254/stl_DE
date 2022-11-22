from DE_Machine_Data_Framework.trigger_pipeline import trigger_pipeline
import pandas as pd


def process_csv(df):
    df["RDT"] = df["RDT"].iloc[0]
    df["Preform ID"] = df["Preform ID"].iloc[0]
    df["Operator"] = df["Operator"].iloc[0]
    df["Time"] = pd.to_datetime(df["Time"], format="%m/%d/%Y %I:%M:%S %p")
    df.dropna(how="all", axis=1, inplace=True)
    df = df.iloc[1:].applymap(str)
    return df


if __name__ == "__main__":
    plant = "Shendra_Glass"
    process = "core_rod_draw"
    table = "core_rod_draw"
    machines = [f"RDT{i:>02}" for i in range(1, 3)]

    trigger_pipeline(plant, process, table, machines, process_csv)
