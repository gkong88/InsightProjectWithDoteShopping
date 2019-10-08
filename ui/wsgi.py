from ui import flask_app as app
if __name__ == "__main__":
    print("RUNNING SERVER")
    app.run(host='0.0.0.0', port = 80)
