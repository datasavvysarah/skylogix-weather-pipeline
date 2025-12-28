set -e  # Exit on error

echo "========================================"
echo "SkyLogix Weather Pipeline Setup"
echo "========================================"
echo ""

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored messages
print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠ $1${NC}"
}

# Check if Python is installed
echo "Checking Python installation..."
if command -v python3 &> /dev/null; then
    PYTHON_VERSION=$(python3 --version)
    print_success "Python found: $PYTHON_VERSION"
else
    print_error "Python 3 is not installed. Please install Python 3.8 or higher."
    exit 1
fi

# Check if pip is installed
echo "Checking pip installation..."
if command -v pip3 &> /dev/null; then
    print_success "pip3 is installed"
else
    print_error "pip3 is not installed. Please install pip3."
    exit 1
fi

# Create virtual environment
echo ""
echo "Creating virtual environment..."
if [ ! -d "venv" ]; then
    python3 -m venv venv
    print_success "Virtual environment created"
else
    print_warning "Virtual environment already exists"
fi

# Activate virtual environment
echo "Activating virtual environment..."
source venv/bin/activate
print_success "Virtual environment activated"

# Install Python dependencies
echo ""
echo "Installing Python dependencies..."
pip install --upgrade pip
pip install -r requirements.txt
print_success "Dependencies installed"

# Create necessary directories
echo ""
echo "Creating project directories..."
mkdir -p config
mkdir -p scripts
mkdir -p dags
mkdir -p sql
mkdir -p logs
print_success "Directories created"

# Copy environment file
echo ""
if [ ! -f ".env" ]; then
    echo "Creating .env file from template..."
    cp .env.example .env
    print_warning "Please update .env file with your actual credentials"
else
    print_warning ".env file already exists"
fi

# Check MongoDB connection
echo ""
echo "Checking MongoDB connection..."
python3 -c "
from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv()
try:
    client = MongoClient(os.getenv('MONGODB_URI', 'mongodb://localhost:27017/'), serverSelectionTimeoutMS=3000)
    client.server_info()
    print('✓ MongoDB connection successful')
except Exception as e:
    print(f'✗ MongoDB connection failed: {e}')
    print('Please ensure MongoDB is running and connection details are correct')
" || print_warning "MongoDB connection check failed. Please verify your MongoDB setup."

# Check PostgreSQL connection
echo ""
echo "Checking PostgreSQL connection..."
python3 -c "
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()
try:
    conn = psycopg2.connect(
        host=os.getenv('POSTGRES_HOST', 'localhost'),
        port=os.getenv('POSTGRES_PORT', '5432'),
        database=os.getenv('POSTGRES_DATABASE', 'postgres'),
        user=os.getenv('POSTGRES_USER', 'postgres'),
        password=os.getenv('POSTGRES_PASSWORD', ''),
        connect_timeout=3
    )
    conn.close()
    print('✓ PostgreSQL connection successful')
except Exception as e:
    print(f'✗ PostgreSQL connection failed: {e}')
    print('Please ensure PostgreSQL is running and connection details are correct')
" || print_warning "PostgreSQL connection check failed. Please verify your PostgreSQL setup."

# Create PostgreSQL tables
echo ""
read -p "Do you want to create PostgreSQL tables now? (y/n) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "Creating PostgreSQL tables..."
    psql -h ${POSTGRES_HOST:-localhost} \
         -p ${POSTGRES_PORT:-5432} \
         -U ${POSTGRES_USER:-postgres} \
         -d ${POSTGRES_DATABASE:-skylogix_warehouse} \
         -f sql/create_tables.sql && print_success "Tables created" || print_error "Failed to create tables"
fi

# Initialize Airflow (optional)
echo ""
read -p "Do you want to initialize Airflow? (y/n) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "Initializing Airflow..."
    export AIRFLOW_HOME=$(pwd)/airflow
    airflow db init
    print_success "Airflow initialized"
    
    echo "Creating Airflow admin user..."
    airflow users create \
        --username admin \
        --firstname Admin \
        --lastname User \
        --role Admin \
        --email admin@skylogix.com \
        --password admin
    print_success "Airflow user created (username: admin, password: admin)"
fi

# Summary
echo ""
echo "========================================"
echo "Setup Complete!"
echo "========================================"
echo ""
echo "Next steps:"
echo "1. Update .env file with your API keys and database credentials"
echo "2. Ensure MongoDB and PostgreSQL are running"
echo "3. Run the ingestion script: python3 scripts/ingest_weather.py"
echo "4. Run the ETL script: python3 scripts/transform_load.py"
echo "5. Start Airflow (if installed):"
echo "   - airflow webserver -p 8080"
echo "   - airflow scheduler"
echo ""
echo "For more information, see README.md"
echo ""
