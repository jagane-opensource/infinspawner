import os
import asyncio
from datetime import datetime,timezone,timedelta
import json
import requests
from requests.exceptions import HTTPError
from jupyterhub.spawner import Spawner
from textwrap import dedent
from urllib.parse import urlunparse, urlparse, quote, unquote
from traitlets import (
    Integer,
    Unicode,
)

class InfinSpawner(Spawner):

    prodcode = '9fcazc4rbiwp6ewg4xlt5c8fu'
    xform_name = 'https://github.com/infinstor/jupyterlab' # should populate from customer info

    infinstor_service_name = Unicode(
        "infinstor.com",
        config=True,
        help=dedent(
            """
            service that his jupyterhub is for - infinstor, isstageX, isdemoX
            """
        )
    )

    async def start(self):
        env = super(InfinSpawner, self).get_env()
        for key, value in env.items():
            self.log.info('InfinSpawner.start. env ' + str(key) + ' = ' + str(value))
        if (not self.auth):
            raise ValueError('auth not found. Cannot proceed')

        self.log.info("InfinSpawner.start: id_token=" + str(self.auth.get_id_token()))
        customer_info = self.get_customer_info()
        self.user_name = customer_info['userName']
        self.log.info("InfinSpawner.start: user_name=" + str(self.user_name))
        #self.log.info("InfinSpawner.start: customer_info=" + json.dumps(customer_info))

        if (not 'jupyterInstanceType' in customer_info):
            raise ValueError('jupyterInstanceType not specified')
        self.instance_type = customer_info['jupyterInstanceType']
        self.log.info("InfinSpawner.start: self.instance_type=" + str(self.instance_type))

        experiment_id = self.create_mlflow_experiment('jupyterlab')
        self.log.info('InfinSpawner.start: experiment_id=' + str(experiment_id))
        if (experiment_id == None):
            raise

        self.create_periodic_run(InfinSpawner.xform_name, self.instance_type, experiment_id)

        start_time = round(datetime.now(timezone.utc).timestamp())
        self.log.info('InfinSpawner.start. user_name=' + self.user_name\
                + '. Entering sleep for 15 minutes. start_time=' + str(start_time)
                + ', api_token=' + str(self.api_token))

        loop = asyncio.get_running_loop()
        end_time = loop.time() + 3600.0
        while True:
            self.log.info(datetime.now())
            if ((loop.time() + 60) >= end_time):
                break
            await asyncio.sleep(60)
            self.log.info('InfinSpawner.start. user_name=' + str(self.user_name)\
                    + '. checking if done. time=' + str(round(datetime.now(timezone.utc).timestamp())))
            if (self.check_if_done(experiment_id, start_time) == True):
                self.log.info('InfinSpawner.start: started. user_name=' + str(self.user_name)\
                        + '. host=' + self.ip + ':' + str(self.port))
                return (self.ip, self.port)
            else:
                self.log.info('InfinSpawner.start: not started yet. user_name=' + str(self.user_name)\
                        + '. continuing to wait')
        self.log.error('InfinSpawner.start: failed to startuser_name=' + str(self.user_name)\
                + '. ')
        raise Exception('Start', 'Failed')

    def check_if_done(self, experiment_id, start_time):
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Authorization': 'Bearer ' + self.auth.get_id_token()
            }
        payload = '{"experiment_ids": ["' + str(experiment_id) + '"], "filter": "", "run_view_type": "ACTIVE_ONLY", "max_results": 1, "order_by": []}'

        url = 'https://mlflow.' + self.infinstor_service_name + '/Prod/2.0/mlflow/runs/search'
        try:
            response = requests.post(url, data=payload, headers=headers)
            resp_json = response.json()
            # self.log.info('InfinSpawner.check_if_done: resp=' + json.dumps(resp_json))
            response.raise_for_status()
        except HTTPError as http_err:
            self.log.info(f'HTTP error occurred: {http_err}')
            return False
        except Exception as err:
            self.log.info(f'Other error occurred: {err}')
            return False
        else:
            if ('runs' in resp_json and len(resp_json['runs']) > 0):
                run = resp_json['runs'][0]
                self.log.info('got run=' + json.dumps(run))
                run_info = run['info']
                run_data = run['data']
                if ((int(run_info['start_time'])/1000) > start_time):
                    if (not 'metrics' in run_data or not 'params' in run_data):
                        return None
                    self.port = 0
                    for m in run_data['metrics']:
                        if (m['key'] == 'public_port'):
                            self.port = int(m['value'])
                            break
                    if (self.port == 0):
                        return False
                    for p in run_data['params']:
                        if (p['key'] == 'public_ip'):
                            self.ip = p['value']
                            break
                    return True
                else:
                    self.log.info('Run ' + str(run_info['run_id']) + ' started earlier. Ignoring')
                    return False
            else:
                self.log.info('No runs available')
                return False

    def poll_vm_state(self):
        self.log.info('InfinSpawner.poll_vm_state. user_name =' + str(self.user_name))
        url = 'https://mlflow.' + self.infinstor_service_name + '/api/2.0/mlflow/projects/singlevm-status'
        self.log.info('InfinSpawner.poll_vm_state: user_name=' + str(self.user_name) + ', url=' + str(url))
        if (not self.auth or not self.auth.get_id_token()):
            self.log.info('InfinSpawner.poll_vm_state: user_name=' + str(self.user_name)\
                    + '. Error. id_token not present. unable to check status. returning not-running.')
            return 0

        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Authorization': 'Bearer ' + self.auth.get_id_token()
            }
        self.log.info('InfinSpawner.poll_vm_state: user_name=' + str(self.user_name)\
                + '. headers=' + str(headers))
        payload = '{ "instance_type": "' + self.instance_type + '", "periodic_run_name": "jupyterlab"}'

        try:
            response = requests.post(url, data=payload, headers=headers, timeout=45.0)
            response.raise_for_status()
        except HTTPError as http_err:
            self.log.error(f'HTTP error occurred: {http_err}')
            return 0
        except Exception as err:
            self.log.error(f'Other error occurred: {err}')
            return 0
        else:
            self.log.info('InfinSpawner.poll_vm_state user_name=' + str(self.user_name)\
                    + '. singlevm_status success. VM=')
            resp_json = response.json()
            self.log.info('InfinSpawner.poll_vm_state: user_name=' + str(self.user_name)\
                    + '. resp=' + json.dumps(resp_json))
            if ('status' in resp_json and resp_json['status'] == 'running'):
                self.log.info('InfinSpawner.poll_vm_state user_name=' + str(self.user_name)\
                        + '. VM is running. Success')
                return None
            else:
                self.log.info('InfinSpawner.poll_vm_state user_name=' + str(self.user_name)\
                        + '. VM indeterminate state. Failed')
                return 0

    def poll_notebook_server(self):
        self.log.info('InfinSpawner.poll_notebook_server. user_name =' + str(self.user_name))
        url = 'http://' + self.ip + ':' + str(self.port) + '/'
        try:
            response = requests.get(url, timeout=5)
            response.raise_for_status()
        except HTTPError as http_err:
            if (response.status_code == 405 or response.status_code == 404):
                self.log.error('InfinSpawner.poll_notebook_server: user_name=' + str(self.user_name)\
                        + ' got 404/405. notebook server is alive')
                return None
            else:
                self.log.error(f'InfinSpawner.poll_notebook_server: user_name=' + str(self.user_name)\
                        + ' HTTP error occurred: {http_err}')
                return 0
        except Exception as err:
            self.log.error(f'InfinSpawner.poll_notebook_server: user_name=' + str(self.user_name)\
                    + ' Other error occurred: {err}')
            return 0
        else:
            self.log.info('InfinSpawner.poll_notebook_server user_name=' + str(self.user_name)\
                    + ' success')
            return None

    async def poll(self):
        if (self.poll_notebook_server() == None):
            self.log.info('InfinSpawner.poll. user_name =' + str(self.user_name) + '. Notebook server is up. -> Success')
            # notebook server is up
            return None
        else:
            # notebook server is down
            if (self.poll_vm_state() == None):
                # notebook server is down, but VM is up
                # wait 10 seconds and see if notebook server responds
                await asyncio.sleep(10)
                if (self.poll_notebook_server() == None):
                    # notebook server came back up
                    self.log.info('InfinSpawner.poll. user_name =' + str(self.user_name) + '. Notebook server is back up. -> Success')
                    return None
                else:
                    # notebook server is down, but VM is still up
                    self.log.info('InfinSpawner.poll. user_name =' + str(self.user_name) + '. Notebook server is down. VM is up. -> Fail')
                    self.clear_state()
                    return 0
            else:
                # notebook server is down and VM is also down
                self.log.info('InfinSpawner.poll. user_name =' + str(self.user_name) + '. Notebook server is down. VM is down. -> Fail')
                self.clear_state()
                return 0

    async def stop(self, now=False):
        self.log.info('InfinSpawner.stop')
        url = 'https://mlflow.' + self.infinstor_service_name + '/api/2.0/mlflow/projects/stop-singlevm'
        self.log.info('InfinSpawner.stop: user_name=' + str(self.user_name)\
                + '. url=' + str(url))
        if (not self.auth or not self.auth.get_id_token()):
            self.log.info('InfinSpawner.stop: user_name=' + str(self.user_name)\
                    + '. Error. id_token not present. unable to stop')
            raise ValueError('id_token not present')

        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Authorization': 'Bearer ' + self.auth.get_id_token()
            }
        self.log.info('InfinSpawner.stop: user_name=' + str(self.user_name)\
                + '. headers=' + str(headers))
        payload = '{ "instance_type": "' + self.instance_type + '", "periodic_run_name": "jupyterlab"}'

        try:
            response = requests.post(url, data=payload, headers=headers, timeout=45.0)
            response.raise_for_status()
        except HTTPError as http_err:
            self.log.error(f'HTTP error occurred: {http_err}')
            self.clear_state()
            raise
        except Exception as err:
            self.log.error(f'Other error occurred: {err}')
            self.clear_state()
            raise
        else:
            self.log.info('InfinSpawner.stop user_name=' + str(self.user_name)\
                    + '. success!')
            self.log.info('InfinSpawner.stop: user_name=' + str(self.user_name)\
                    + '. resp=' + str(response.text))
            self.clear_state()

    def load_state(self, state):
        #self.log.info('InfinSpawner.load_state. user_name=' + str(self.user_name))
        self.log.info('InfinSpawner.load_state')
        """Restore state of spawner from database.

        Called for each user's spawner after the hub process restarts.

        `state` is a dict that'll contain the value returned by `get_state` of
        the spawner, or {} if the spawner hasn't persisted any state yet.

        Override in subclasses to restore any extra state that is needed to track
        the single-user server for that user. Subclasses should call super().
        """
        super().load_state(state)
        if ('ip' in state):
            self.ip = state['ip']
        else:
            self.ip = ''
        if ('port' in state):
            self.port = state['port']
        else:
            self.port = 0
        if ('user_name' in state):
            self.user_name = state['user_name']
        else:
            self.user_name = ''

    def get_state(self):
        self.log.info('InfinSpawner.get_state. user_name=' + str(self.user_name))
        """Save state of spawner into database.

        A black box of extra state for custom spawners. The returned value of this is
        passed to `load_state`.

        Subclasses should call `super().get_state()`, augment the state returned from
        there, and return that state.

        Returns
        -------
        state: dict
             a JSONable dict of state
        """
        state = super().get_state()
        if (self.ip and len(self.ip) > 0):
            state['ip'] = self.ip
        if (self.port and self.port > 0):
            state['port'] = self.port
        if (self.user_name):
            state['user_name'] = self.user_name
        return state

    def clear_state(self):
        self.log.info('InfinSpawner.clear_state. user_name=' + str(self.user_name))
        """Clear any state that should be cleared when the single-user server stops.

        State that should be preserved across single-user server instances should not be cleared.

        Subclasses should call super, to ensure that state is properly cleared.
        """
        super().clear_state()
        self.ip = ''
        self.port = 0
        self.user_name = ''
        self.auth = None

    def set_auth(self, auth):
        self.auth = auth

    def get_customer_info(self):
        if (not self.auth or not self.auth.get_id_token()):
            self.log.info('InfinSpawner.get_customer_info: user_name=' + str(self.user_name)\
                    + '. Error. id_token not present')
            raise ValueError('id_token not present')
        payload = ("ProductCode=" + InfinSpawner.prodcode + "&clientType=jupyterhub")
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Authorization': 'Bearer ' + self.auth.get_id_token()
            }

        url = 'https://api.' + self.infinstor_service_name + '/customerinfo'
        self.log.info('InfinSpawner.get_customer_info: url=' + str(url))
        self.log.info('InfinSpawner.get_customer_info: headers=' + str(headers))

        try:
            response = requests.post(url, data=payload, headers=headers)
            response.raise_for_status()
        except HTTPError as http_err:
            self.log.error(f'HTTP error occurred: {http_err}')
            raise
        except Exception as err:
            self.log.error(f'Other error occurred: {err}')
            raise
        else:
            self.log.info('InfinSpawner.get_customer_info success!')
            self.log.info('InfinSpawner.get_customer_info: resp=' + str(response.text))
            return response.json()

    def create_mlflow_experiment(self, experiment_name):
        if (not self.auth or not self.auth.get_id_token()):
            self.log.info('InfinSpawner.create_mlflow_experiment: user_name=' + str(self.user_name)\
                    + '. Error. id_token not present')
            raise ValueError('id_token not present')
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Authorization': 'Bearer ' + self.auth.get_id_token()
            }

        url = 'https://mlflow.' + self.infinstor_service_name +\
                '/Prod/2.0/mlflow/experiments/get-by-name?experiment_name=' +\
                quote(experiment_name)
        try:
            response = requests.get(url, headers=headers)
            resp_json = response.json()
            response.raise_for_status()
        except HTTPError as http_err:
            self.log.info(f'HTTP error occurred in get-by-name: {http_err}')
        except Exception as err:
            self.log.info(f'Other error occurred in get-by-name: {err}')
        else:
            self.log.info('create_mlflow_experiment: get-by-name success')
            return resp_json['experiment']['experiment_id']

        payload = '{ "name": "' + experiment_name + '" }'
        headers = {
                'Content-Type': 'application/x-www-form-urlencoded',
                'Authorization': 'Bearer ' + self.auth.get_id_token()
            }

        url = 'https://mlflow.' + self.infinstor_service_name + '/Prod/2.0/mlflow/experiments/create'
        try:
            response = requests.post(url, data=payload, headers=headers)
            resp_json = response.json()
            response.raise_for_status()
        except HTTPError as http_err:
            self.log.info(f'HTTP error occurred: {http_err}')
            return None
        except Exception as err:
            self.log.info(f'Other error occurred: {err}')
            return None
        else:
            self.log.info('InfinSpawner: create_mlflow_experiment success. user_name=' + str(self.user_name)\
                    + '. Trying to set persistent tag')

        experiment_id = resp_json['experiment_id']
        payload = '{ "experiment_id": "' + str(experiment_id) + '", "key": "persistent", "value": "true" }'
        url = 'https://mlflow.' + self.infinstor_service_name + '/Prod/2.0/mlflow/experiments/set-experiment-tag'
        try:
            response = requests.post(url, data=payload, headers=headers)
            resp_json = response.json()
            response.raise_for_status()
        except HTTPError as http_err:
            self.log.info(f'HTTP error occurred: {http_err}')
            return None
        except Exception as err:
            self.log.info(f'Other error occurred: {err}')
            return None
        else:
            self.log.info('InfinSpawner: set-experiment-tag success')


        return experiment_id

    def create_periodic_run(self, xformname, itype, experiment_id):
        if (not self.auth or not self.auth.get_id_token()):
            self.log.info('InfinSpawner.create_periodic_run: user_name=' + str(self.user_name)\
                    + '. Error. id_token not present')
            raise ValueError('id_token not present')
        now_utc = datetime.now(timezone.utc)
        now_utc = now_utc + timedelta(seconds=120)
        tt = now_utc.timetuple()
        dtms = str(tt[4]) + '_' + "{:02d}".format(tt[3]) + '_' + str(tt[2]) + '_' + str(tt[1]) + '_*_' + str(tt[0])

        pr = {}
        pr['name'] = 'jupyterlab'
        pr['period'] = {'type': 'once', 'value': dtms}
        pr['data'] = {'type': 'no-input-data'}
        pr['transforms'] = [{'transform_name': xformname, 'transform_number': 1}]
        pr['runtarget'] = {'location': 'aws', 'type': 'singlevm', 'instance_type': itype}
        pr['params'] = {'positional': [],
                'kwargs': [{'key': 'token', 'value': str(self.api_token)}]}
        pr['experiment_id'] = str(experiment_id)

        jpr = json.dumps(pr)
        self.log.info('InfinSpawner.start. json =' + jpr)

        payload = 'periodicRunName=jupyterlab&clientType=jupyterhub&periodicRunJson=' + quote(jpr)

        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Authorization': 'Bearer ' + self.auth.get_id_token()
            }

        url = 'https://api.' + self.infinstor_service_name + '/addmodifyperiodicrun'
        self.log.info('InfinSpawner.create_periodic_run: url=' + str(url))
        self.log.info('InfinSpawner.create_periodic_run: headers=' + str(headers))

        try:
            response = requests.post(url, data=payload, headers=headers)
            response.raise_for_status()
        except HTTPError as http_err:
            self.log.error(f'HTTP error occurred: {http_err}')
            raise
        except Exception as err:
            self.log.error(f'Other error occurred: {err}')
            raise
        else:
            self.log.info('InfinSpawner.create_periodic_run user_name=' + str(self.user_name)\
                    + '. success!')
            self.log.info('InfinSpawner.create_periodic_run: user_name=' + str(self.user_name)\
                    + '. resp=' + str(response.text))
            return response.json()

