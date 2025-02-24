import json
import requests

class ShopifyETLPipeline:
    def fetch_shopify_data(self):
        print("DEBUG: THIS IS THE BRAND NEW MAIN_T2.PY WE CREATED WITH SHORT-CIRCUIT FIX")

        # For demonstration, let's call the real API for just one page,
        # so we see if we can replicate or fix the error quickly.
        params = {"status": "any", "limit": 5, "order": "asc"}
        response = requests.get(
            "https://dj4qxk-yt.myshopify.com/admin/api/2024-01/orders.json",
            auth=("5dd52f10564583d30a3a0eccfbb47bb9", "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"),  # or pass your actual token-based auth
            params=params
        )
        data = response.json()

        all_marketing_consent = []

        orders = data.get("orders", [])
        print(f"DEBUG: We got {len(orders)} orders from the real API")

        for idx, order in enumerate(orders, start=1):
            customer = order.get("customer", None)
            print(f"DEBUG: ORDER #{idx} => customer is:", customer)

            if not isinstance(customer, dict):
                print("DEBUG: Skipping because customer is None or not dict.")
                continue

            # If we're here, customer is definitely a dict
            print("DEBUG: Customer is a dict =>", customer)

            # Short-circuit approach: if 'sms_marketing_consent' is None, we do (None or {}) => {}
            # So .get("state","") happens on {} instead of None.
            sms_consent = (customer.get("sms_marketing_consent") or {}).get("state", "")
            email_consent = (customer.get("email_marketing_consent") or {}).get("state", "")

            print(f"DEBUG: email_consent={email_consent}, sms_consent={sms_consent}")

            if email_consent or sms_consent:
                all_marketing_consent.append({
                    "customer_id": str(customer["id"]),
                    "email_consent": email_consent,
                    "sms_consent": sms_consent
                })

        print("DEBUG: Done collecting marketing_consent:", all_marketing_consent)
        return all_marketing_consent

if __name__ == "__main__":
    pipeline = ShopifyETLPipeline()
    pipeline.fetch_shopify_data()