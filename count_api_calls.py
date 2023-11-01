import json

def count_total_api_calls(filename):
    # Load the JSON data from the file
    with open(filename, 'r') as file:
        data = json.load(file)
    
    # Initialize a variable to keep track of the total API calls
    total_api_calls = 0
    
    # Iterate through each object in the JSON data
    for obj in data:
        # Iterate through each item in the "API_Calls_Info" list
        for info in obj.get('API_Calls_Info', []):
            # Add the "API_Calls_Per_Chunk" value to the total
            total_api_calls += info.get('API_Calls_Per_Chunk', 0)
    
    return total_api_calls

# Call the function and print the result
filename = 'process_log.json'  # Replace with the name of your JSON file
total_api_calls = count_total_api_calls(filename)
print(f'Total API Calls Per Chunk: {total_api_calls}')
