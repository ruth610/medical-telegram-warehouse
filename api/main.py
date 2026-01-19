from fastapi import FastAPI

app = FastAPI(title="Medical Data Warehouse API")

@app.get("/")
def read_root():
    return {"message": "Welcome to the Medical Data Warehouse API"}
