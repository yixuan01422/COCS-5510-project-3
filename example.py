selected_columns = ['id', 'name', 'age']
result_rows = [
    [2, 'Alice', 25],
    [4, 'Dog', 19],
    [3, 'Cat', 50]
]

# Sorting by age ASC
order_column = 'age'
ascending = False

if order_column in selected_columns:
    order_col_idx = selected_columns.index(order_column)
    result_rows = sorted(result_rows, key=lambda x: x[order_col_idx], reverse=not ascending)

print(result_rows)

