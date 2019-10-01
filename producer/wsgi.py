from kafka_ingest import application
if __name__ == "__main__":
    print("RUNNIGN SERVER")
    application.run(host='0.0.0.0', port = 5001)
