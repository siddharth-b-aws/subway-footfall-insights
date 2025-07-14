import pandas as pd
import os

input_file = "data\\raw\\MTA_Subway_Hourly_Ridership__2020-2024.csv"
output_file = "data\\raw\\MTA_Subway_Hourly_Ridership_1GB.csv"
target_size = 1 * 100**3  # 1 GB in bytes

# First, read a chunk to estimate average row size
sample = pd.read_csv(input_file, nrows=10000)
sample.to_csv("temp_sample.csv", index=False)
sample_size = os.path.getsize("temp_sample.csv")
avg_row_size = sample_size / len(sample)
estimated_rows = int(target_size / avg_row_size)

print(f"Average row size: {avg_row_size:.2f} bytes")
print(f"Estimated rows needed for ~1 GB: {estimated_rows}")

# Now read the required number of rows
df = pd.read_csv(input_file, nrows=estimated_rows)
df.to_csv(output_file, index=False)

print(f"Saved sampled 1GB dataset to: {output_file}")
