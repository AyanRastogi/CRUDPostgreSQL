import json
import sys
import shlex
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, insert, select, update, delete

# Load DB config from config.json
with open("config.json") as f:
    config = json.load(f)

DB_USER = config["db_user"]
DB_PASSWORD = config["db_password"]
DB_HOST = config["db_host"]
DB_PORT = config["db_port"]
DB_NAME = config["db_name"]

# Connect to PostgreSQL
engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
metadata = MetaData()


# Define the users table
users = Table(
    "users",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("name", String),
    Column("email", String)
)

metadata.create_all(engine)
conn = engine.connect()

# CRUD Operations
def create_user(name, email):
    stmt = insert(users).values(name=name, email=email)
    result = conn.execute(stmt)
    return {"status": "ok", "inserted_id": result.inserted_primary_key[0]}

def read_users():
    stmt = select(users)
    result = conn.execute(stmt)
    return [dict(row) for row in result.mappings()]

def update_user(user_id, name=None, email=None):
    stmt = update(users).where(users.c.id == user_id)
    values = {}
    if name:
        values['name'] = name
    if email:
        values['email'] = email
    if values:
        result = conn.execute(stmt.values(**values))
        return {"status": "ok", "updated": result.rowcount}
    return {"status": "error", "message": "No fields to update."}

def delete_user(user_id):
    stmt = delete(users).where(users.c.id == user_id)
    result = conn.execute(stmt)
    return {"status": "ok", "deleted": result.rowcount}

# Command parser for MCP format
def parse_command(command_line):
    try:
        args = shlex.split(command_line)
        action = args[0].upper()

        if action == "CREATE" and args[1].upper() == "USER":
            params = dict(arg.split("=", 1) for arg in args[2:])
            return create_user(name=params.get("name"), email=params.get("email"))

        elif action == "READ" and args[1].upper() == "USERS":
            return read_users()

        elif action == "UPDATE" and args[1].upper() == "USER":
            params = dict(arg.split("=", 1) for arg in args[2:])
            return update_user(user_id=int(params["id"]), name=params.get("name"), email=params.get("email"))

        elif action == "DELETE" and args[1].upper() == "USER":
            params = dict(arg.split("=", 1) for arg in args[2:])
            return delete_user(user_id=int(params["id"]))

        else:
            return {"status": "error", "message": "Unknown command."}
    except Exception as e:
        return {"status": "error", "message": str(e)}

# Main MCP loop
def mcp_loop():
    print("MCP CRUD Server Ready", flush=True)
    while True:
        try:
            command = sys.stdin.readline()
            if not command:
                break  # EOF
            response = parse_command(command.strip())
            if command.lower() in ("exit"):
                print("Exiting MCP CRUD Agent...", flush=True)
                break
            response = parse_command(command)
            print(json.dumps(response), flush=True)
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(json.dumps({"status": "error", "message": str(e)}), flush=True)

if __name__ == "__main__":
    mcp_loop()

