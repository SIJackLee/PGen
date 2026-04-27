SUPABASE_URL = "https://ompufmezugftzoergdbn.supabase.co"
SERVICE_ROLE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im9tcHVmbWV6dWdmdHpvZXJnZGJuIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2NDc4OTMyMywiZXhwIjoyMDgwMzY1MzIzfQ.6Nx96KcMHIm4qV_AdzM7gkkBHsoj8RNe32EIJ5EVklU"

HEADERS = {
    "apikey": SERVICE_ROLE_KEY,
    "Authorization": f"Bearer {SERVICE_ROLE_KEY}",
    "Content-Type": "application/json",
    "Accept-Profile": "public",
    "Content-Profile": "public",
}