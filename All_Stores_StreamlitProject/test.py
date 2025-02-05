import requests

url = "https://apps.qeapps.com/ecom_apps_n/production/demand-forecasting/api/v1/getAllGraphqlOrders?page=1&limit=250&domain=sweetsnob.myshopify.com"

response = requests.get(url)

print(f"Status Code: {response.status_code}")
print(f"Response: {response.text}")  # Optional: Print response body
