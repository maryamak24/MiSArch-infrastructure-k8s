import subprocess
import sys
import time
import socket
import urllib.request
import urllib3
import json
import signal
import argparse
import os
import datetime
import base64
import shutil
import requests
from sseclient import SSEClient
from influxdb_client import InfluxDBClient
from influxdb_client.client.exceptions import InfluxDBError
from database_snapshot_creator.snapshot_db import snapshot_mongodb

KUBECTL_NAMESPACE = "misarch"
PORT_FORWARDS = [
    ("svc/misarch-experiment-executor", 4000, 8888),
    ("svc/influxdb", 4001, 80),
	("pod/inventory-db-0", 27017, 27017)
]
URL = f"http://127.0.0.1:{PORT_FORWARDS[0][1]}"
EXPERIMENTS_DIR = os.path.normpath(os.path.join(os.path.dirname(__file__), '..'))
REPO_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', '..'))

PRODUCT_QUERY='''query MyQuery {
  products {
    nodes {
      defaultVariant {
        inventoryCount
      }
    }
  }
}'''


# Keycloak
REALM = "Misarch"
CLIENT_ID = "frontend"
USERNAME = "gatling"
PASSWORD = "123"

# InfluxDB
INFLUX_ORG = "misarch"
INFLUX_BUCKET = "gatling"
INFLUX_URL = f"http://127.0.0.1:{PORT_FORWARDS[1][1]}"


def make_path_absolute(base_path: str, filename: str)->str:
	if os.path.isabs(filename):
		return filename
	else:
		return os.path.join(base_path, filename)
	
def read_validate_json(file_path: str):
	with open(file_path, 'r') as f:
		try:
			conf_obj = json.load(f)
			return conf_obj
		except Exception as e:
			raise RuntimeError(f"Failed to read/parse config JSON: {e}")

def read_and_b64_encode(file_path: str):
	with open(file_path, 'r') as f:
		try:
			data = f.read()
			encoded = base64.b64encode(bytes(data, "utf-8")).decode("ascii")
			return encoded
		except Exception as e:
			raise RuntimeError(f"Unexpected error reading file {file_path}: {e}")
		

def copy_file_to_destination(src_path: str, destination_root: str, base_dir: str | None = None):
	abs_src = os.path.abspath(src_path)
	if base_dir:
		abs_base = os.path.abspath(base_dir)
		if os.path.commonpath([abs_base, abs_src]) == abs_base:
			rel_path = os.path.relpath(abs_src, abs_base)
		else:
			rel_path = os.path.basename(abs_src)
	else:
		rel_path = os.path.basename(abs_src)

	dest_path = os.path.join(destination_root, rel_path)
	os.makedirs(os.path.dirname(dest_path), exist_ok=True)
	shutil.copy2(abs_src, dest_path)
	return dest_path


def copy_experiment_files(experiment_json_path: str, experiment_config: dict, experiment_path: str):
	"""Copy the experiment JSON and all files referenced by this experiment into experiment_path."""
	os.makedirs(experiment_path, exist_ok=True)
	copy_file_to_destination(experiment_json_path, experiment_path, EXPERIMENTS_DIR)

	for config_key in ("chaosConfig", "misarchConfig"):
		file_ref = experiment_config.get(config_key)
		if file_ref:
			copy_file_to_destination(make_path_absolute(EXPERIMENTS_DIR, os.path.join("profiles",file_ref)), experiment_path, EXPERIMENTS_DIR)

	for variant in experiment_config.get("gatlingConfig", []):
		for script_key in ("loadScript", "userSteps"):
			file_ref = variant.get(script_key)
			if file_ref:
				copy_file_to_destination(make_path_absolute(EXPERIMENTS_DIR, os.path.join("profiles",file_ref)), experiment_path, EXPERIMENTS_DIR)


def login(cluster_url: str):
    url = f"{cluster_url}/keycloak/realms/{REALM}/protocol/openid-connect/token"
    data = urllib.parse.urlencode({
        "grant_type": "password",
        "client_id": CLIENT_ID,
        "username": USERNAME,
        "password": PASSWORD,
    }).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=data,
        headers={
            "Content-Type": "application/x-www-form-urlencoded"
        },
        method="POST",
    )
    with urllib.request.urlopen(request) as response:
        return json.loads(response.read().decode("utf-8"))

def refresh(cluster_url: str, refresh_token: str):
    url = f"{cluster_url}/keycloak/realms/{REALM}/protocol/openid-connect/token"
    data = urllib.parse.urlencode({
        "grant_type": "refresh_token",
        "client_id": CLIENT_ID,
        "refresh_token": refresh_token,
    }).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=data,
        headers={
            "Content-Type": "application/x-www-form-urlencoded"
        },
        method="POST",
    )

    with urllib.request.urlopen(request) as response:
        return json.loads(response.read().decode("utf-8"))


def graphql_query(cluster_url: str, tokens, query, variables=None):
    payload = json.dumps({
        "query": query,
        "variables": variables or {}
    }).encode("utf-8")

    def send_request(access_token):
        request = urllib.request.Request(
            f"{cluster_url}/api/graphql",
            data=payload,
            headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
            },
            method="POST",
        )

        with urllib.request.urlopen(request) as response:
            return json.loads(response.read().decode("utf-8"))

    try:
        return send_request(tokens["access_token"])

    except urllib.error.HTTPError as e:
        if e.code != 401:
            raise

        print("Access token expired. Refreshing...")
        new_tokens = refresh(tokens["refresh_token"])
        # Update the existing dict so the caller automatically has the new tokens
        tokens.update(new_tokens)
        return send_request(tokens["access_token"])
	
def export_influxdb_to_csv(e_id: str, e_version: str, output_path: str):
	token = read_terraform_output("influxdb_admin_token")
	with InfluxDBClient(url=INFLUX_URL, token=token, org=INFLUX_ORG) as client:
		query = f'from(bucket:"{INFLUX_BUCKET}") |> range(start: 0) |> filter(fn:(r) => r.testUUID == "{e_id}")'
		try:
			query_client = client.query_api()
			response = query_client.query_raw(query=query, org=INFLUX_ORG)
		except InfluxDBError as e:
			raise RuntimeError(f"Influx query failed: {e}")
	
		csv_text = response.data.decode("utf-8").rstrip()
	
		if not csv_text:
			raise RuntimeError("Influx query retruned no data")
	
		with open(output_path, "x") as f:
			f.write(csv_text)
			f.write("\n")

	
def read_terraform_output(name: str, terraform_dir=None):
	"""Read a Terraform output value from the repository root or given directory."""
	if terraform_dir is None:
		terraform_dir = REPO_ROOT
	cmd = ["terraform", "output", "-raw", name]
	try:
		result = subprocess.run(cmd, cwd=terraform_dir, capture_output=True, text=True, check=True)
		return result.stdout.strip()
	except FileNotFoundError:
		raise RuntimeError("terraform command not found; ensure Terraform is installed and on PATH")
	except subprocess.CalledProcessError as e:
		stderr = e.stderr.strip() if e.stderr else str(e)
		raise RuntimeError(f"Failed to read Terraform output '{name}': {stderr}")
		
		
def start_port_forward(svc: str, local_port: int, remote_port: int):
	'''Set up a port forward using kubectl.'''
	cmd = [
		"kubectl",
		"port-forward",
		"-n",
		KUBECTL_NAMESPACE,
		svc,
		f"{local_port}:{remote_port}",
	]
	try:
		p = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
		return p
	except FileNotFoundError:
		print("kubectl not found in PATH", file=sys.stderr)
		return None


def start_port_forwards(forwards):
	processes = []
	for svc, local_port, remote_port in forwards:
		print(f"Starting port-forward -n {KUBECTL_NAMESPACE} {svc} {local_port}:{remote_port}...")
		p = start_port_forward(svc, local_port, remote_port)
		if p is None:
			continue
		processes.append((svc, local_port, p))
	return processes


def wait_for_port_forward_processes(processes, timeout: float = 10.0):
	for svc, local_port, proc in processes:
		if proc.poll() is not None:
			print(f"Port-forward for {svc} exited immediately.", file=sys.stderr)
			continue
		if not wait_for_port("127.0.0.1", local_port, timeout=timeout):
			print(f"Port-forward for {svc} did not open port {local_port} within timeout", file=sys.stderr)


def wait_for_port(host: str, port: int, timeout: float = 10.0):
	deadline = time.time() + timeout
	while time.time() < deadline:
		try:
			with socket.create_connection((host, port), timeout=1):
				return True
		except Exception:
			time.sleep(0.2)
	return False
	
def generate_experiment()->tuple[str,str]:
	'''Generates a new experiment with default values. To override the default load configuration specify a gatling config'''
	url = f"{URL}/experiment/generate?loadType=NormalLoadTest&testDuration=60&maximumArrivingUsersPerSecond=3"
	req = urllib.request.Request(url, method="POST")
	try:
		with urllib.request.urlopen(req, timeout=5) as resp:
			raw = resp.read()
			try:
				data = json.loads(raw)
			except Exception:
				data = raw.decode(errors="replace")
			return tuple(str(data).split(':'))
	except Exception as e:
		raise RuntimeError(f"Failed to generate experiment: {e}")
	

def set_json_config(config: str, e_id: str, e_version: str, url: str):
	'''Set a JSON config (Chaos Toolkit, MiSArch or global)
	
	Arguments:
	config: relative path to the config file from experiments directory or absolute path
	url: URL of the API endpoint
	'''
	conf_path = make_path_absolute(EXPERIMENTS_DIR, config)
	
	if not os.path.exists(conf_path):
		raise FileNotFoundError(f"Config not found: {conf_path}")
	
	conf_obj = read_validate_json(conf_path)
	
	body = json.dumps(conf_obj).encode('utf-8')
	headers = {"Content-Type": "application/json"}
	req = urllib.request.Request(url, data=body, headers=headers, method="PUT")
	try:
		with urllib.request.urlopen(req, timeout=10) as resp:
			raw = resp.read()
			try:
				return json.loads(raw)
			except Exception:
				return raw.decode(errors='replace')
	except Exception as e:
		raise RuntimeError((f"Failed to update config at {url}: {e}"))
		

def set_gatling_config(config: str, e_id: str, e_version: str, url: str):
	'''Set the gatling config of an experiment. Kotlin scripts and user steps CSV are base64 encoded and packaged into a JSON body'''
	conf_obj = []
	for variant in config:
		script_path = make_path_absolute(EXPERIMENTS_DIR, os.path.join("profiles", variant['loadScript']))
		user_path = make_path_absolute(EXPERIMENTS_DIR, os.path.join("profiles", variant['userSteps']))
		
		if not os.path.exists(script_path) or not os.path.exists(user_path):
			raise FileNotFoundError(f"Gatling config not found: {script_path} or {user_path}")
		filename = variant["loadScript"].split("/")[-1][:-3]
		variant_obj = {}
		variant_obj['fileName']= filename
		variant_obj['encodedWorkFileContent'] = read_and_b64_encode(script_path)
		variant_obj['encodedUserStepsFileContent'] = read_and_b64_encode(user_path)
		conf_obj.append(variant_obj)
		
	body = json.dumps(conf_obj).encode('utf-8')
	headers = {"Content-Type": "application/json"}
	req = urllib.request.Request(url, data=body, headers=headers, method="PUT")
	try:
		with urllib.request.urlopen(req, timeout=10) as resp:
			raw = resp.read()
			try:
				return json.loads(raw)
			except Exception:
				return raw.decode(errors='replace')
	except Exception as e:
		raise RuntimeError((f"Failed to update config at {url}: {e}"))
	

def start_experiment(e_id: str, e_version: str):
	start_url=f"{URL}/experiment/{e_id}/{e_version}/start"
	start_req=urllib.request.Request(start_url, method="POST")
	try:
		with urllib.request.urlopen(start_req, timeout=10) as res:
			if res.code == 200:
				print("Experiment started sucessfully")
			else:
				raise RuntimeError("Failed to start experiment")
	except Exception as e:
		raise RuntimeError(f"Failed to start experiment")
	

def wait_for_event(e_id: str, e_version: str):
	"""Wait for the first server sent event which signifies the experiment is completed
	Note: This works (somehow) but only because the function returns an requests.exceptions.MissingSchema
	"""
	event_url = f"{URL}/experiment/{e_id}/{e_version}/events"
	headers = {"Accept": "text/event-stream"}
	
	def with_urllib3(url: str):
		"""Get a streaming response for the given event feed using urllib3."""
		http = urllib3.PoolManager()
		res = http.request('GET', url, preload_content=False, headers=headers)
		yield from SSEClient(res).events()
		
	try:
		for msg in with_urllib3(event_url):
			print(msg)
			return
	except requests.exceptions.MissingSchema as e:
		return
	
	
def run_experiment(config):
	e_id, e_version = generate_experiment()
	print(f"Created experiment {e_id}:{e_version}")
	set_json_config(
		os.path.join('profiles',config['chaosConfig']),
		e_id,
		e_version,
		f"{URL}/experiment/{e_id}/{e_version}/chaosToolkitConfig")
	set_json_config(
		os.path.join('profiles',config['misarchConfig']),
		e_id,
		e_version,
		f"{URL}/experiment/{e_id}/{e_version}/misarchExperimentConfig"
    )
	set_gatling_config(
		config['gatlingConfig'],
		e_id,
		e_version,
		f"{URL}/experiment/{e_id}/{e_version}/gatlingConfig" )
	
	start_experiment(e_id, e_version)
	wait_for_event(e_id, e_version)

	return (e_id, e_version)
        

def cleanup_port_forward_processes(processes):
	for svc, local_port, proc in processes:
		if proc.poll() is None:
			proc.terminate()
			try:
				proc.wait(timeout=2)
			except Exception:
				proc.kill()


def main():
	port_forward_processes = []

	def _cleanup(signum=None, frame=None):
		cleanup_port_forward_processes(port_forward_processes)
		if signum:
			sys.exit(0)

	signal.signal(signal.SIGINT, _cleanup)
	signal.signal(signal.SIGTERM, _cleanup)

	# CLI: expect a filename (relative to the top-level `experiments/` folder)
	parser = argparse.ArgumentParser(prog="Experiment runner", description="Run automated experiments against the MiSArch system")
	parser.add_argument('-f', '--file', required=True, help='Filename inside the experiments directory')
	args = parser.parse_args()

	path = make_path_absolute(EXPERIMENTS_DIR, args.file)

	try:
		cluster_url = read_terraform_output("global_domain")
		print(f"Terraform global_domain: {cluster_url}")
	except RuntimeError as e:
		print(f"Warning: could not read Terraform global_domain: {e}", file=sys.stderr)
		cluster_url = None

	port_forward_processes = start_port_forwards(PORT_FORWARDS)
	if not port_forward_processes:
		print("Skipping port-forwards; attempting to contact localhost directly.")
	else:
		wait_for_port_forward_processes(port_forward_processes, timeout=10)

	print(f"Opening experiment file: {path}")

	credentials = login(cluster_url)

	try:
		with open(path, 'r') as f:
			d = json.load(f)
			results_path = os.path.abspath(os.path.normpath(os.path.join(os.path.dirname(__file__), '..', 'results')))

			#inventory_db_password = read_terraform_output("mongodb_root_password_inventory")
			#inventory_db_path = f"mongodb://root:{inventory_db_password}@localhost:27017/?directConnection=true&authSource=admin"
			for experiment in d['experiments']:
				print(f"Running experiment {experiment['testName']}")
				start_products=graphql_query(cluster_url, credentials, PRODUCT_QUERY)
				#pre_inventory= snapshot_mongodb(inventory_db_path)

				start_time=datetime.datetime.now()
				e_id, e_version = run_experiment(experiment)
				end_time=datetime.datetime.now()

				experiment_path = os.path.join(results_path, f"{e_id}:{e_version}")
				copy_experiment_files(path, experiment, experiment_path)
				export_influxdb_to_csv(e_id, e_version, os.path.join(experiment_path, "results.csv"))
				#post_inventory = snapshot_mongodb(inventory_db_path)
				end_products=graphql_query(cluster_url, credentials, PRODUCT_QUERY)
				print(f"Experiment {e_id}:{e_version} finished after {(end_time-start_time).total_seconds()}s")
				consistency_results= {}
				consistency_results['before']=start_products['data']['products']['nodes'][0]['defaultVariant']
				consistency_results['after']=end_products['data']['products']['nodes'][0]['defaultVariant']
				with open(os.path.join(experiment_path, "consistency.json"), 'x') as f:
					f.write(json.dumps(consistency_results))

				#pre_inventory_json=json.dumps(pre_inventory, indent=2, ensure_ascii=False, default=str)
				#post_inventory_json=json.dumps(post_inventory, indent=2, ensure_ascii=False, default=str)
	except FileNotFoundError as e:
		print(e)
		sys.exit(2)
	except json.JSONDecodeError as e:
		print(f"Failed to parse JSON: {e}", file=sys.stderr)
		sys.exit(3)
	finally:
		cleanup_port_forward_processes(port_forward_processes)
		
        
if __name__ == "__main__":
	main()


