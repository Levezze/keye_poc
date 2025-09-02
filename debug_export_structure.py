"""Debug what the concentration analyzer returns vs what export expects"""
import pandas as pd
from core.deterministic.concentration import ConcentrationAnalyzer

# Create test data
data = {
    "date": ["2023-01-15", "2023-02-20"],
    "entity": ["Company_A", "Company_B"], 
    "revenue": [1000.50, 500.00]
}
df = pd.DataFrame(data)

analyzer = ConcentrationAnalyzer()
result = analyzer.analyze(
    df=df,
    group_by="entity",
    value_column="revenue"
)

print("Concentration analyzer returns:")
print(f"Type: {type(result.data)}")
print(f"Keys: {result.data.keys()}")
print("\nStructure:")
for key, value in result.data.items():
    print(f"{key}: {type(value)}")
    if isinstance(value, dict):
        print(f"  -> {list(value.keys())}")