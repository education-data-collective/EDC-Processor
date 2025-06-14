import firebase_admin
from firebase_admin import credentials, firestore
import os
import sys
from typing import Dict, List, Any, Optional

class FirestoreReader:
    """
    A class to handle read-only operations with Google Firestore.
    """
    
    def __init__(self, service_account_path: str = None):
        """
        Initialize Firestore connection.
        
        Args:
            service_account_path: Path to the service account JSON file.
                                If None, will look for GOOGLE_APPLICATION_CREDENTIALS env var
                                or 'firebase-service-account.json' in current directory.
        """
        self.db = None
        self._initialize_firebase(service_account_path)
    
    def _initialize_firebase(self, service_account_path: str = None):
        """Initialize Firebase Admin SDK with service account credentials."""
        try:
            # Check if Firebase is already initialized
            if firebase_admin._apps:
                print("Firebase already initialized, using existing app.")
                self.db = firestore.client()
                return
            
            # Determine service account path
            if service_account_path is None:
                # Check environment variable first
                service_account_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
                
                # If not found, check for default file name
                if service_account_path is None:
                    default_path = 'firebase-service-account.json'
                    if os.path.exists(default_path):
                        service_account_path = default_path
                    else:
                        raise FileNotFoundError(
                            "Service account file not found. Please:\n"
                            "1. Set GOOGLE_APPLICATION_CREDENTIALS environment variable, or\n"
                            "2. Place your service account JSON file as 'firebase-service-account.json' in current directory, or\n"
                            "3. Pass the path directly to the constructor"
                        )
            
            # Verify file exists
            if not os.path.exists(service_account_path):
                raise FileNotFoundError(f"Service account file not found: {service_account_path}")
            
            # Initialize Firebase
            cred = credentials.Certificate(service_account_path)
            firebase_admin.initialize_app(cred)
            self.db = firestore.client()
            
            print(f"✅ Successfully connected to Firestore using: {service_account_path}")
            
        except Exception as e:
            print(f"❌ Failed to initialize Firebase: {str(e)}")
            sys.exit(1)
    
    def test_connection(self) -> bool:
        """
        Test the Firestore connection by attempting to list collections.
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            # Try to get collections (this will fail if no connection)
            collections = list(self.db.collections())
            print(f"✅ Connection test successful! Found {len(collections)} collections.")
            return True
        except Exception as e:
            print(f"❌ Connection test failed: {str(e)}")
            return False
    
    def list_collections(self) -> List[str]:
        """
        List all collections in the Firestore database.
        
        Returns:
            List of collection names
        """
        try:
            collections = self.db.collections()
            collection_names = [col.id for col in collections]
            print(f"📁 Collections found: {collection_names}")
            return collection_names
        except Exception as e:
            print(f"❌ Error listing collections: {str(e)}")
            return []
    
    def read_collection(self, collection_name: str, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Read documents from a specific collection.
        
        Args:
            collection_name: Name of the collection to read from
            limit: Maximum number of documents to retrieve (default: 10)
            
        Returns:
            List of document dictionaries
        """
        try:
            collection_ref = self.db.collection(collection_name)
            docs = collection_ref.limit(limit).stream()
            
            documents = []
            for doc in docs:
                doc_data = doc.to_dict()
                doc_data['_id'] = doc.id  # Add document ID
                documents.append(doc_data)
            
            print(f"📄 Retrieved {len(documents)} documents from '{collection_name}'")
            return documents
            
        except Exception as e:
            print(f"❌ Error reading collection '{collection_name}': {str(e)}")
            return []
    
    def read_document(self, collection_name: str, document_id: str) -> Optional[Dict[str, Any]]:
        """
        Read a specific document by ID.
        
        Args:
            collection_name: Name of the collection
            document_id: ID of the document to retrieve
            
        Returns:
            Document data as dictionary, or None if not found
        """
        try:
            doc_ref = self.db.collection(collection_name).document(document_id)
            doc = doc_ref.get()
            
            if doc.exists:
                doc_data = doc.to_dict()
                doc_data['_id'] = doc.id
                print(f"📄 Document '{document_id}' found in '{collection_name}'")
                return doc_data
            else:
                print(f"❌ Document '{document_id}' not found in '{collection_name}'")
                return None
                
        except Exception as e:
            print(f"❌ Error reading document '{document_id}' from '{collection_name}': {str(e)}")
            return None
    
    def query_collection(self, collection_name: str, field: str, operator: str, value: Any, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Query a collection with a simple where clause.
        
        Args:
            collection_name: Name of the collection to query
            field: Field name to filter on
            operator: Query operator ('==', '>', '<', '>=', '<=', '!=', 'in', 'not-in', 'array-contains')
            value: Value to compare against
            limit: Maximum number of documents to retrieve
            
        Returns:
            List of matching documents
        """
        try:
            collection_ref = self.db.collection(collection_name)
            query = collection_ref.where(field, operator, value).limit(limit)
            docs = query.stream()
            
            documents = []
            for doc in docs:
                doc_data = doc.to_dict()
                doc_data['_id'] = doc.id
                documents.append(doc_data)
            
            print(f"🔍 Query found {len(documents)} documents in '{collection_name}' where {field} {operator} {value}")
            return documents
            
        except Exception as e:
            print(f"❌ Error querying collection '{collection_name}': {str(e)}")
            return []


def main():
    """
    Main function to test Firestore connection and operations.
    """
    print("🔥 Firebase Firestore Connection Test")
    print("=" * 40)
    
    # Initialize Firestore reader
    # You can pass a specific path: FirestoreReader('path/to/your/service-account.json')
    firestore_reader = FirestoreReader()
    
    # Test connection
    if not firestore_reader.test_connection():
        print("Failed to connect to Firestore. Exiting.")
        return
    
    print("\n" + "=" * 40)
    
    # List all collections
    print("📁 Listing all collections:")
    collections = firestore_reader.list_collections()
    
    if not collections:
        print("No collections found or unable to access collections.")
        return
    
    print("\n" + "=" * 40)
    
    # Read from the first collection (if any exist)
    if collections:
        first_collection = collections[0]
        print(f"📖 Reading from collection: '{first_collection}'")
        documents = firestore_reader.read_collection(first_collection, limit=3)
        
        if documents:
            print("\nSample documents:")
            for i, doc in enumerate(documents, 1):
                print(f"\n📄 Document {i}:")
                for key, value in doc.items():
                    print(f"  {key}: {value}")
        
        print("\n" + "=" * 40)
        
        # Example query (uncomment and modify as needed)
        # print("🔍 Example query:")
        # query_results = firestore_reader.query_collection(
        #     first_collection, 
        #     'some_field',    # Replace with actual field name
        #     '==', 
        #     'some_value',    # Replace with actual value
        #     limit=5
        # )
    
    print("\n✅ Firestore test completed!")


if __name__ == "__main__":
    main() 