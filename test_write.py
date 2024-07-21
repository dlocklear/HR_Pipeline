import os

# Ensure the directory exists
os.makedirs("data", exist_ok=True)

output_file = os.path.abspath("data/test_write.csv")

# Remove the file if it already exists
if os.path.exists(output_file):
    os.remove(output_file)

# Write a simple CSV file
with open(output_file, "w") as f:
    f.write("col1,col2\n")
    f.write("1,2\n")
    f.write("3,4\n")

print(f"Test file written to {output_file}")
