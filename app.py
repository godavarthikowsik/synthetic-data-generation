import os
import pandas as pd
import numpy as np
from faker import Faker
from flask import Flask, request, render_template, send_file, redirect, url_for, session, flash
from kaggle.api.kaggle_api_extended import KaggleApi
from pymongo import MongoClient
from werkzeug.security import generate_password_hash, check_password_hash
from datetime import datetime


app = Flask(__name__)
app.secret_key = "64dd3b3a6a3a83a2b37f60617d195fce825105d4e50972f04a98403ce9b60c68"  # Required for session management
faker = Faker()

# MongoDB Connection
MONGO_URI = "mongodb+srv://godavarthikowsik:Kousik3466@cluster0.pz0up.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
client = MongoClient(MONGO_URI)
db = client["synthetic_data_generator"]
users_collection = db["users"]
history_collection = db["generation_history"]

# Authenticate Kaggle API (No manual credentials needed)
api = KaggleApi()
api.authenticate()


# Function to fetch dataset schema
def get_dataset_schema(dataset_name):
    search_results = api.dataset_list(search=dataset_name)
    if not search_results:
        return None, None

    dataset_ref = search_results[0]
    dataset_files = api.dataset_list_files(dataset_ref.ref).files
    csv_files = [f.name for f in dataset_files if f.name.endswith(".csv")]

    if not csv_files:
        return None, None

    csv_filename = csv_files[0]
    api.dataset_download_file(dataset_ref.ref, csv_filename, path=".")

    df = pd.read_csv(csv_filename, nrows=500)
    schema = {
        "fields": {col: "numerical" if pd.api.types.is_numeric_dtype(df[col]) else "categorical" for col in df.columns}
    }

    return df, schema


# Function to generate synthetic data
def generate_synthetic_data(schema, num_rows=1000):
    synthetic_data = {col: np.random.normal(loc=50, scale=15, size=num_rows).astype(int) if col_type == "numerical"
                      else [faker.word() for _ in range(num_rows)] for col, col_type in schema["fields"].items()}
    return pd.DataFrame(synthetic_data)


# Routes
@app.route("/")
def home():
    return render_template("home.html")


@app.route("/generate", methods=["GET", "POST"])
def generate():
    if "user" not in session:
        return redirect(url_for("login"))

    if request.method == "POST":
        dataset_name = request.form["dataset_name"]
        df, schema = get_dataset_schema(dataset_name)

        if df is None or schema is None:
            return "Dataset schema could not be inferred. Try another dataset."

        synthetic_data = generate_synthetic_data(schema)
        filename = f"synthetic_data_{dataset_name}_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
        filepath = os.path.join("generated_files", filename)

        os.makedirs("generated_files", exist_ok=True)
        synthetic_data.to_csv(filepath, index=False)

        # Store history in MongoDB
        history_collection.insert_one({
            "username": session["user"],
            "dataset_name": dataset_name,
            "file_path": filename,
            "timestamp": datetime.now()
        })

        return send_file(filepath, as_attachment=True)

    return render_template("generate.html")


@app.route("/history")
def history():
    if "user" not in session:
        return redirect(url_for("login"))

    user_history = list(history_collection.find({"username": session["user"]}).sort("timestamp", -1))
    return render_template("history.html", history=user_history)


@app.route("/download/<filename>")
def download(filename):
    if "user" not in session:
        return redirect(url_for("login"))

    filepath = os.path.join("generated_files", filename)
    if os.path.exists(filepath):
        return send_file(filepath, as_attachment=True)

    flash("File not found!", "danger")
    return redirect(url_for("history"))


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form["username"]
        password = request.form["password"]

        user = users_collection.find_one({"username": username})
        if user and check_password_hash(user["password"], password):
            session["user"] = username
            return redirect(url_for("generate"))

        flash("Invalid username or password", "danger")

    return render_template("login.html")


@app.route("/signup", methods=["GET", "POST"])
def signup():
    if request.method == "POST":
        username = request.form["username"]
        phone = request.form["phone"]
        password = request.form["password"]
        confirm_password = request.form["confirm_password"]

        # Check if passwords match
        if password != confirm_password:
            flash("Passwords do not match!", "danger")
            return redirect(url_for("signup"))

        # Check if the user already exists
        if users_collection.find_one({"username": username}):
            flash("User already exists!", "warning")
            return redirect(url_for("signup"))

        # Hash password and save user details
        hashed_password = generate_password_hash(password)
        users_collection.insert_one({
            "username": username,
            "phone": phone,
            "password": hashed_password
        })

        flash("Account created successfully! Please log in.", "success")
        return redirect(url_for("login"))

    return render_template("signup.html")



@app.route("/logout")
def logout():
    session.pop("user", None)
    return redirect(url_for("home"))


if __name__ == "__main__":
    app.run(debug=True)
