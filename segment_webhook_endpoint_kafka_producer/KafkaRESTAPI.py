from flask import Flask, request
from flask_restful import Resource, Api

print("Hello World")

# Initialize central registry for your app
# this includes your functions,
# URL rules
app = Flask(__name__)
# entry point for your application
api = Api(app)

todos = {"0": "0 lives here"}

# implement the abstract class
# of your RESTful resource.
# restful resource is an object with a
# type, associated data,
# relationship to other resources
# and methods to operate on it

class TodoSimple(Resource):
    def get(self, todo_id):
        """
        returns lookup of todo_id from todos
        :param todo_id:
        :return:
        """
        return {todo_id: todos[todo_id]}

    # TODO: you probably don't need this
    def put(self, todo_id):
        """

        #TODO remove this
        The PUT method requests that the enclosed entity be stored under the supplied URI. If the URI refers to an already existing resource, it is modified; if the URI does not point to an existing resource, then the server can create the resource with that URI.[24]

        :param topo_id:
        :return:
        """
        todos[todo_id] = request.form['data']
        return {todo_id: todos[todo_id]}
    def post(self):
        """

        # TODO: remove this
        about POST:
        The POST method requests that the server accept the entity enclosed in the request as a new subordinate of the web resource identified by the URI. The data POSTed might be, for example, an annotation for existing resources; a message for a bulletin board, newsgroup, mailing list, or comment thread; a block of data that is the result of submitting a web form to a data-handling process; or an item to add to a database.[23]

        about SEGMENT POST: A Webhook is an HTTP callback: a simple event-notification via HTTP POST. A web application implementing Webhooks will POST a message to a URL when certain things happen. The exact code required to make HTTP POST requests will depend on your application’s software language and architecture.
        :return:
        """
        pass

api.add_resource(HelloWorld, '/')
# api.add_resource(TodoSimple, '/')

if __name__ == '__main__':
    app.run(debug = True)

print("Goodbye World")