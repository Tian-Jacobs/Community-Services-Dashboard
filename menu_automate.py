#!/usr/bin/env python3
"""
Municipal Complaints Database CLI
A command-line interface for querying municipal complaints data
"""

import sqlite3
import csv
import os
from datetime import datetime
from typing import List, Tuple

class ComplaintsDatabase:
    def __init__(self, db_name: str = "complaints.db"):
        self.db_name = db_name
        self.conn = None
        
    def connect(self):
        """Connect to SQLite database"""
        self.conn = sqlite3.connect(self.db_name)
        self.conn.row_factory = sqlite3.Row  # Enable column access by name
        
    def disconnect(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            
    def create_tables(self):
        """Create database tables"""
        cursor = self.conn.cursor()
        
        # Create residents table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS residents (
                resident_id INTEGER PRIMARY KEY,
                first_name TEXT NOT NULL,
                last_name TEXT NOT NULL,
                ward INTEGER NOT NULL,
                email TEXT,
                phone TEXT
            )
        ''')
        
        # Create service_categories table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS service_categories (
                category_id INTEGER PRIMARY KEY,
                category_name TEXT NOT NULL
            )
        ''')
        
        # Create complaints table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS complaints (
                complaint_id INTEGER PRIMARY KEY,
                resident_id INTEGER NOT NULL,
                category_id INTEGER NOT NULL,
                title TEXT NOT NULL,
                description TEXT,
                submission_date DATE NOT NULL,
                FOREIGN KEY (resident_id) REFERENCES residents (resident_id),
                FOREIGN KEY (category_id) REFERENCES service_categories (category_id)
            )
        ''')
        
        # Create status_logs table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS status_logs (
                log_id INTEGER PRIMARY KEY,
                complaint_id INTEGER NOT NULL,
                status TEXT NOT NULL,
                status_date DATE NOT NULL,
                FOREIGN KEY (complaint_id) REFERENCES complaints (complaint_id)
            )
        ''')
        
        self.conn.commit()
        
    def load_csv_data(self, csv_files: dict):
        """Load data from CSV files into database tables"""
        cursor = self.conn.cursor()
        
        # Load residents
        if 'residents' in csv_files and os.path.exists(csv_files['residents']):
            with open(csv_files['residents'], 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file, delimiter=';')
                for row in reader:
                    cursor.execute('''
                        INSERT OR REPLACE INTO residents 
                        (resident_id, first_name, last_name, ward, email, phone)
                        VALUES (?, ?, ?, ?, ?, ?)
                    ''', (row['resident_id'], row['first_name'], row['last_name'], 
                          row['ward'], row['email'], row['phone']))
        
        # Load service categories
        if 'categories' in csv_files and os.path.exists(csv_files['categories']):
            with open(csv_files['categories'], 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file, delimiter=';')
                for row in reader:
                    cursor.execute('''
                        INSERT OR REPLACE INTO service_categories 
                        (category_id, category_name)
                        VALUES (?, ?)
                    ''', (row['category_id'], row['category_name']))
        
        # Load complaints
        if 'complaints' in csv_files and os.path.exists(csv_files['complaints']):
            with open(csv_files['complaints'], 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file, delimiter=';')
                for row in reader:
                    cursor.execute('''
                        INSERT OR REPLACE INTO complaints 
                        (complaint_id, resident_id, category_id, title, description, submission_date)
                        VALUES (?, ?, ?, ?, ?, ?)
                    ''', (row['complaint_id'], row['resident_id'], row['category_id'],
                          row['title'], row['description'], row['submission_date']))
        
        # Load status logs
        if 'status_logs' in csv_files and os.path.exists(csv_files['status_logs']):
            with open(csv_files['status_logs'], 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file, delimiter=';')
                for row in reader:
                    cursor.execute('''
                        INSERT OR REPLACE INTO status_logs 
                        (log_id, complaint_id, status, status_date)
                        VALUES (?, ?, ?, ?)
                    ''', (row['log_id'], row['complaint_id'], row['status'], row['status_date']))
        
        self.conn.commit()
        
    def execute_query(self, query: str, params: tuple = ()) -> List[sqlite3.Row]:
        """Execute a SQL query and return results"""
        cursor = self.conn.cursor()
        cursor.execute(query, params)
        return cursor.fetchall()
        
    def print_results(self, results: List[sqlite3.Row], title: str = "Query Results"):
        """Print query results in a formatted table"""
        print(f"\n{'='*60}")
        print(f"{title}")
        print(f"{'='*60}")
        
        if not results:
            print("No results found.")
            return
            
        # Get column names
        columns = results[0].keys()
        
        # Print header
        header = " | ".join(f"{col:15}" for col in columns)
        print(header)
        print("-" * len(header))
        
        # Print rows
        for row in results:
            row_str = " | ".join(f"{str(row[col]):15}" for col in columns)
            print(row_str)
            
        print(f"\nTotal records: {len(results)}")

class ComplaintsCLI:
    def __init__(self):
        self.db = ComplaintsDatabase()
        
    def initialize_database(self):
        """Initialize database with CSV data"""
        print("Initializing database...")
        self.db.connect()
        self.db.create_tables()
        
        # Define CSV file paths (update these paths as needed)
        csv_files = {
            'residents': 'residents.csv',
            'categories': 'service_categories.csv', 
            'complaints': 'complaints.csv',
            'status_logs': 'status_logs.csv'
        }
        
        self.db.load_csv_data(csv_files)
        print("Database initialized successfully!")
        
    def show_menu(self):
        """Display the main menu"""
        print("\n" + "="*60)
        print("MUNICIPAL COMPLAINTS DATABASE - QUERY MENU")
        print("="*60)
        print("1.  View all active complaints")
        print("2.  View complaints by category")
        print("3.  View complaints by ward")
        print("4.  View resident complaint history")
        print("5.  View complaint resolution statistics")
        print("6.  View overdue complaints (submitted over 30 days ago)")
        print("7.  View complaints by status")
        print("8.  View top complaint categories")
        print("9.  View ward performance summary")
        print("10. View complaint timeline for specific complaint")
        print("0.  Exit")
        print("="*60)
        
    def query_1_active_complaints(self):
        """View all active complaints"""
        query = '''
            SELECT 
                c.complaint_id,
                r.first_name || ' ' || r.last_name as resident_name,
                sc.category_name,
                c.title,
                c.submission_date,
                sl.status
            FROM complaints c
            JOIN residents r ON c.resident_id = r.resident_id
            JOIN service_categories sc ON c.category_id = sc.category_id
            JOIN (
                SELECT complaint_id, status, 
                       ROW_NUMBER() OVER (PARTITION BY complaint_id ORDER BY status_date DESC) as rn
                FROM status_logs
            ) sl ON c.complaint_id = sl.complaint_id AND sl.rn = 1
            WHERE sl.status != 'Resolved'
            ORDER BY c.submission_date DESC
        '''
        results = self.db.execute_query(query)
        self.db.print_results(results, "Active Complaints")
        
    def query_2_complaints_by_category(self):
        """View complaints by category"""
        print("\nAvailable categories:")
        cat_query = "SELECT category_id, category_name FROM service_categories ORDER BY category_id"
        categories = self.db.execute_query(cat_query)
        for cat in categories:
            print(f"{cat['category_id']}. {cat['category_name']}")
            
        try:
            category_id = input("\nEnter category ID: ")
            query = '''
                SELECT 
                    c.complaint_id,
                    r.first_name || ' ' || r.last_name as resident_name,
                    c.title,
                    c.submission_date,
                    sl.status
                FROM complaints c
                JOIN residents r ON c.resident_id = r.resident_id
                JOIN service_categories sc ON c.category_id = sc.category_id
                JOIN (
                    SELECT complaint_id, status, 
                           ROW_NUMBER() OVER (PARTITION BY complaint_id ORDER BY status_date DESC) as rn
                    FROM status_logs
                ) sl ON c.complaint_id = sl.complaint_id AND sl.rn = 1
                WHERE c.category_id = ?
                ORDER BY c.submission_date DESC
            '''
            results = self.db.execute_query(query, (category_id,))
            cat_name = next((cat['category_name'] for cat in categories if str(cat['category_id']) == category_id), "Unknown")
            self.db.print_results(results, f"Complaints for {cat_name}")
        except ValueError:
            print("Invalid category ID entered.")
            
    def query_3_complaints_by_ward(self):
        """View complaints by ward"""
        try:
            ward = input("Enter ward number: ")
            query = '''
                SELECT 
                    c.complaint_id,
                    r.first_name || ' ' || r.last_name as resident_name,
                    sc.category_name,
                    c.title,
                    c.submission_date,
                    sl.status
                FROM complaints c
                JOIN residents r ON c.resident_id = r.resident_id
                JOIN service_categories sc ON c.category_id = sc.category_id
                JOIN (
                    SELECT complaint_id, status, 
                           ROW_NUMBER() OVER (PARTITION BY complaint_id ORDER BY status_date DESC) as rn
                    FROM status_logs
                ) sl ON c.complaint_id = sl.complaint_id AND sl.rn = 1
                WHERE r.ward = ?
                ORDER BY c.submission_date DESC
            '''
            results = self.db.execute_query(query, (ward,))
            self.db.print_results(results, f"Complaints for Ward {ward}")
        except ValueError:
            print("Invalid ward number entered.")
            
    def query_4_resident_history(self):
        """View resident complaint history"""
        try:
            resident_id = input("Enter resident ID: ")
            query = '''
                SELECT 
                    c.complaint_id,
                    sc.category_name,
                    c.title,
                    c.submission_date,
                    sl.status
                FROM complaints c
                JOIN service_categories sc ON c.category_id = sc.category_id
                JOIN (
                    SELECT complaint_id, status, 
                           ROW_NUMBER() OVER (PARTITION BY complaint_id ORDER BY status_date DESC) as rn
                    FROM status_logs
                ) sl ON c.complaint_id = sl.complaint_id AND sl.rn = 1
                WHERE c.resident_id = ?
                ORDER BY c.submission_date DESC
            '''
            results = self.db.execute_query(query, (resident_id,))
            
            # Get resident name
            name_query = "SELECT first_name || ' ' || last_name as name FROM residents WHERE resident_id = ?"
            name_result = self.db.execute_query(name_query, (resident_id,))
            resident_name = name_result[0]['name'] if name_result else f"Resident {resident_id}"
            
            self.db.print_results(results, f"Complaint History for {resident_name}")
        except ValueError:
            print("Invalid resident ID entered.")
            
    def query_5_resolution_statistics(self):
        """View complaint resolution statistics"""
        query = '''
            SELECT 
                sc.category_name,
                COUNT(*) as total_complaints,
                SUM(CASE WHEN sl.status = 'Resolved' THEN 1 ELSE 0 END) as resolved,
                SUM(CASE WHEN sl.status = 'In Progress' THEN 1 ELSE 0 END) as in_progress,
                SUM(CASE WHEN sl.status = 'Submitted' THEN 1 ELSE 0 END) as submitted,
                ROUND(
                    (SUM(CASE WHEN sl.status = 'Resolved' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)), 2
                ) as resolution_rate
            FROM complaints c
            JOIN service_categories sc ON c.category_id = sc.category_id
            JOIN (
                SELECT complaint_id, status, 
                       ROW_NUMBER() OVER (PARTITION BY complaint_id ORDER BY status_date DESC) as rn
                FROM status_logs
            ) sl ON c.complaint_id = sl.complaint_id AND sl.rn = 1
            GROUP BY sc.category_id, sc.category_name
            ORDER BY total_complaints DESC
        '''
        results = self.db.execute_query(query)
        self.db.print_results(results, "Complaint Resolution Statistics by Category")
        
    def query_6_overdue_complaints(self):
        """View overdue complaints (over 30 days old)"""
        query = '''
            SELECT 
                c.complaint_id,
                r.first_name || ' ' || r.last_name as resident_name,
                sc.category_name,
                c.title,
                c.submission_date,
                sl.status,
                CAST((julianday('now') - julianday(c.submission_date)) AS INTEGER) as days_old
            FROM complaints c
            JOIN residents r ON c.resident_id = r.resident_id
            JOIN service_categories sc ON c.category_id = sc.category_id
            JOIN (
                SELECT complaint_id, status, 
                       ROW_NUMBER() OVER (PARTITION BY complaint_id ORDER BY status_date DESC) as rn
                FROM status_logs
            ) sl ON c.complaint_id = sl.complaint_id AND sl.rn = 1
            WHERE sl.status != 'Resolved' 
            AND julianday('now') - julianday(c.submission_date) > 30
            ORDER BY days_old DESC
        '''
        results = self.db.execute_query(query)
        self.db.print_results(results, "Overdue Complaints (Over 30 Days)")
        
    def query_7_complaints_by_status(self):
        """View complaints by status"""
        print("\nAvailable statuses: Submitted, In Progress, Resolved")
        status = input("Enter status: ").strip()
        
        query = '''
            SELECT 
                c.complaint_id,
                r.first_name || ' ' || r.last_name as resident_name,
                sc.category_name,
                c.title,
                c.submission_date,
                sl.status_date as last_status_date
            FROM complaints c
            JOIN residents r ON c.resident_id = r.resident_id
            JOIN service_categories sc ON c.category_id = sc.category_id
            JOIN (
                SELECT complaint_id, status, status_date,
                       ROW_NUMBER() OVER (PARTITION BY complaint_id ORDER BY status_date DESC) as rn
                FROM status_logs
            ) sl ON c.complaint_id = sl.complaint_id AND sl.rn = 1
            WHERE sl.status = ?
            ORDER BY c.submission_date DESC
        '''
        results = self.db.execute_query(query, (status,))
        self.db.print_results(results, f"Complaints with Status: {status}")
        
    def query_8_top_complaint_categories(self):
        """View top complaint categories"""
        query = '''
            SELECT 
                sc.category_name,
                COUNT(*) as complaint_count,
                ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM complaints), 2) as percentage
            FROM complaints c
            JOIN service_categories sc ON c.category_id = sc.category_id
            GROUP BY sc.category_id, sc.category_name
            ORDER BY complaint_count DESC
        '''
        results = self.db.execute_query(query)
        self.db.print_results(results, "Top Complaint Categories")
        
    def query_9_ward_performance(self):
        """View ward performance summary"""
        query = '''
            SELECT 
                r.ward,
                COUNT(*) as total_complaints,
                SUM(CASE WHEN sl.status = 'Resolved' THEN 1 ELSE 0 END) as resolved,
                SUM(CASE WHEN sl.status = 'In Progress' THEN 1 ELSE 0 END) as in_progress,
                SUM(CASE WHEN sl.status = 'Submitted' THEN 1 ELSE 0 END) as submitted,
                ROUND(
                    (SUM(CASE WHEN sl.status = 'Resolved' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)), 2
                ) as resolution_rate
            FROM complaints c
            JOIN residents r ON c.resident_id = r.resident_id
            JOIN (
                SELECT complaint_id, status, 
                       ROW_NUMBER() OVER (PARTITION BY complaint_id ORDER BY status_date DESC) as rn
                FROM status_logs
            ) sl ON c.complaint_id = sl.complaint_id AND sl.rn = 1
            GROUP BY r.ward
            ORDER BY total_complaints DESC
        '''
        results = self.db.execute_query(query)
        self.db.print_results(results, "Ward Performance Summary")
        
    def query_10_complaint_timeline(self):
        """View complaint timeline for specific complaint"""
        try:
            complaint_id = input("Enter complaint ID: ")
            
            # First show complaint details
            detail_query = '''
                SELECT 
                    c.complaint_id,
                    r.first_name || ' ' || r.last_name as resident_name,
                    sc.category_name,
                    c.title,
                    c.description,
                    c.submission_date
                FROM complaints c
                JOIN residents r ON c.resident_id = r.resident_id
                JOIN service_categories sc ON c.category_id = sc.category_id
                WHERE c.complaint_id = ?
            '''
            details = self.db.execute_query(detail_query, (complaint_id,))
            
            if not details:
                print(f"No complaint found with ID {complaint_id}")
                return
                
            detail = details[0]
            print(f"\n{'='*60}")
            print(f"COMPLAINT DETAILS")
            print(f"{'='*60}")
            print(f"ID: {detail['complaint_id']}")
            print(f"Resident: {detail['resident_name']}")
            print(f"Category: {detail['category_name']}")
            print(f"Title: {detail['title']}")
            print(f"Description: {detail['description']}")
            print(f"Submitted: {detail['submission_date']}")
            
            # Now show status timeline
            timeline_query = '''
                SELECT 
                    status,
                    status_date
                FROM status_logs
                WHERE complaint_id = ?
                ORDER BY status_date ASC
            '''
            timeline = self.db.execute_query(timeline_query, (complaint_id,))
            self.db.print_results(timeline, f"Status Timeline for Complaint {complaint_id}")
            
        except ValueError:
            print("Invalid complaint ID entered.")
            
    def run(self):
        """Main CLI loop"""
        print("Welcome to the Municipal Complaints Database CLI!")
        
        try:
            self.initialize_database()
            
            while True:
                self.show_menu()
                choice = input("\nEnter your choice (0-10): ").strip()
                
                if choice == '0':
                    print("Goodbye!")
                    break
                elif choice == '1':
                    self.query_1_active_complaints()
                elif choice == '2':
                    self.query_2_complaints_by_category()
                elif choice == '3':
                    self.query_3_complaints_by_ward()
                elif choice == '4':
                    self.query_4_resident_history()
                elif choice == '5':
                    self.query_5_resolution_statistics()
                elif choice == '6':
                    self.query_6_overdue_complaints()
                elif choice == '7':
                    self.query_7_complaints_by_status()
                elif choice == '8':
                    self.query_8_top_complaint_categories()
                elif choice == '9':
                    self.query_9_ward_performance()
                elif choice == '10':
                    self.query_10_complaint_timeline()
                else:
                    print("Invalid choice. Please enter a number between 0-10.")
                    
                input("\nPress Enter to continue...")
                
        except KeyboardInterrupt:
            print("\n\nExiting...")
        except Exception as e:
            print(f"An error occurred: {e}")
        finally:
            self.db.disconnect()

if __name__ == "__main__":
    cli = ComplaintsCLI()
    cli.run()