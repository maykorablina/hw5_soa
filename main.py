from fastapi import FastAPI

app = FastAPI(title="HW5 SOA API")

@app.get("/health")
async def health_check():
    return {"status": "ok"}

@app.get("/")
async def root():
    return {"message": "Welcome to HW5 SOA"}
