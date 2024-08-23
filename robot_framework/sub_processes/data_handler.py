"""Module that """
import re
import pyodbc
import requests
from requests_ntlm import HttpNtlmAuth


class DatabaseHandler:
    """
    Class to handle database operations.
    """

    def __init__(self, connection_string):
        """
        Initialize the DatabaseHandler with server and database names.
        """
        self.connection_string = connection_string
        self.connection = None

    def open_connection(self):
        """
        Open a connection to the SQL Server database.
        """
        self.connection = pyodbc.connect(self.connection_string)

    def close_connection(self):
        """
        Close the connection to the SQL Server database.
        """
        if self.connection:
            self.connection.close()

    def fetch_records(self, query):
        """
        Execute a query and fetch all records.
        """
        with self.connection.cursor() as cursor:
            cursor.execute(query)
            columns = [column[0] for column in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def execute_stored_procedure(self, procedure_name, params):
        """
        Execute a stored procedure with the given parameters.
        """
        with self.connection.cursor() as cursor:
            cursor.execute(f"EXEC {procedure_name} {params}")


class APIClient:
    """
    Class to handle API requests.
    """

    def __init__(self, username, password):
        """
        Initialize the APIClient with NTLM authentication credentials.
        """
        self.auth = HttpNtlmAuth(username, password)

    def get_form_digest(self, url):
        """
        Get form digest value by making an API POST request.
        """
        headers = {
            "Content-Type": "application/json; charset=UTF-8"
        }
        try:
            response = requests.post(url, headers=headers, auth=self.auth, timeout=60)
            response.raise_for_status()
            return re.search(r'formDigestValue":"([^"]+)"', response.text).group(1)
        except requests.exceptions.RequestException as e:
            print(f"Request failed. Error message: {str(e)}")
            return None

    def post_data(self, url, headers, body):
        """
        Post data to a given URL.
        """
        try:
            response = requests.post(url, headers=headers, json=body, auth=self.auth, timeout=60)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Request failed. Error message: {str(e)}")
            return None


class TaxonomyHandler:
    """MISSING"""
    def __init__(self, username, password, base_url, endpoint, patterns):
        self.session = self.create_session(username, password)
        self.base_url = base_url
        self.endpoint = endpoint
        self.patterns = patterns

    def create_session(self, username, password):
        """Create a session with NTLM authentication."""
        session = requests.Session()
        session.auth = HttpNtlmAuth(username, password)
        session.headers.update({"Content-Type": "application/json"})
        return session

    def fetch_data(self, url):
        """Fetch data from the given URL using the session."""
        response = self.session.post(url)
        response.raise_for_status()
        return response.json()

    def process_data(self, initial_url):
        """Fetch, categorize, and save data from the SharePoint API."""
        all_rows = []
        next_url = initial_url

        while next_url:
            json_data = self.fetch_data(next_url)
            all_rows.extend(json_data.get("Row", []))

            next_url = f"{self.base_url}{self.endpoint}{json_data.get('NextHref', '')}" if "NextHref" in json_data else None

        return all_rows
