import linecache
import json

# Make sure that requests is installed in your WSL
import requests 

# Open the file and count the number of lines
# with open('./output.txt', 'r') as f:
#     end = sum(1 for _ in f)

#set starting id and ending id
start = 1
end = 50

# Loop over the JSON file
i=start

while i <= end:     
    
    # read a specific line
    line = linecache.getline('./output.txt', i)
    #print(line)
    # write the line to the API
    myjson = json.loads(line)
    
    print(myjson)
    
    response = requests.post('http://localhost:80/invoiceitem', json=myjson)

    #print("Status code: ", response.status_code)
    #print("Printing Entire Post Request")
    print(response.json())

    # increase i
    i+=1
