import requests

# Replace with the actual GraphQL endpoint
API_ENDPOINT = "https://api.spire.com/graphql"

# Optional: replace with your real token if needed
HEADERS = {
    "Content-Type": "application/json",
    # "Authorization": "Bearer YOUR_TOKEN"
}

QUERY = """
query {
    lng_traffic: portEventsByLocation(
        location: {
            unlocode: "NLRTM"
        }
        timeRange: {
            startTime: "2023-11-02T00:00:00Z"
            endTime: "2025-07-02T23:59:59Z"
        }
        state: OPEN
        first: 100
    ) {
        nodes {
            id
            vessel {
                id
                staticData {
                    callsign
                    imo
                    mmsi
                    name
                    shipType
                }
            }
            location {
                name
                unlocode
            }
            ata
            atd
            draughtAta
            draughtAtd
            draughtChange
            duration {
                iso
                seconds
            }
            state
            timestamp
            updateTimestamp
        }
        pageInfo {
            hasNextPage
            endCursor
        }
        totalCount
    }
}
"""

def run_query(query):
    response = requests.post(API_ENDPOINT, json={'query': query}, headers=HEADERS)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Query failed with status code {response.status_code}: {response.text}")

# Run the query and print the result
data = run_query(QUERY)
print(data)
