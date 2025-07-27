
import requests

url = "https://drive.google.com/uc?export=download&id=1XnQXcHFUxOejcGcuf632IMGE8teC-Xkb"
output_path = "flight_model_app.zip"

print("Downloading model package...")
response = requests.get(url)
with open(output_path, "wb") as f:
    f.write(response.content)

print("Download complete. Extract the zip and run app.py")
