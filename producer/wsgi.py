from segment_ingest import application
if __name__ == "__main__":
    print("RUNNING SERVER")
    application.run(host='0.0.0.0', port = 5000)
