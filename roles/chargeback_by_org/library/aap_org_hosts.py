#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Collect every host an organization automated during a window."""

from __future__ import absolute_import, division, print_function

__metaclass__ = type

DOCUMENTATION = r'''
---
module: aap_org_hosts
short_description: Hosts automated by an AAP organization during a window
description:
  - Walks every job an organization ran in a window and collects the hosts
    each one touched, from the job host summaries.
  - The Controller exposes job host summaries only as a sub-list of a job,
    so this is one request per job. Those requests are independent and
    I/O bound, so they are issued concurrently; done sequentially through
    the uri module the same walk takes roughly a second per job.
  - Reads host names from the summary rows rather than from inventory, so
    hosts automated during the window and since deleted are still counted.
options:
  base_url:
    description: Base URL of the AAP gateway or Controller.
    required: true
    type: str
  api_base_path:
    description: API path prefix. Use /api/v2 for a pre-gateway Controller.
    default: /api/controller/v2
    type: str
  username:
    description: Controller username.
    required: true
    type: str
  password:
    description: Controller password.
    required: true
    type: str
  validate_certs:
    description: Whether to verify the Controller's TLS certificate.
    default: true
    type: bool
  organization_id:
    description: Controller organization id. Gateway ids are a different space.
    required: true
    type: int
  created_after:
    description: ISO8601 lower bound on job creation, inclusive.
    required: true
    type: str
  created_before:
    description: ISO8601 upper bound on job creation, inclusive.
    required: true
    type: str
  page_size:
    description: API page size. The Controller caps this at 200 by default.
    default: 200
    type: int
  threads:
    description: Concurrent requests against the Controller.
    default: 16
    type: int
  timeout:
    description: Per-request timeout in seconds.
    default: 30
    type: int
author:
  - Shadowman
'''

EXAMPLES = r'''
- name: Collect the hosts Infrastructure automated in July
  aap_org_hosts:
    base_url: https://aap.example.com
    username: "{{ controller_user }}"
    password: "{{ controller_password }}"
    organization_id: 1
    created_after: "2026-07-01T00:00:00Z"
    created_before: "2026-07-31T23:59:59.999999Z"
  register: july
'''

RETURN = r'''
hostnames:
  description: Sorted unique host names this organization automated.
  returned: always
  type: list
  elements: str
deleted_hostnames:
  description:
    - Host names that appear only on summary rows whose host reference is
      null, meaning the host has since been removed from inventory. These
      are invisible to any query that starts from the hosts endpoint.
  returned: always
  type: list
  elements: str
job_count:
  description: Number of jobs walked.
  returned: always
  type: int
summary_count:
  description: Number of job host summary rows read.
  returned: always
  type: int
'''

import json
import ssl
from concurrent.futures import ThreadPoolExecutor

from ansible.module_utils.basic import AnsibleModule
from ansible.module_utils.six.moves.urllib.error import HTTPError, URLError
from ansible.module_utils.six.moves.urllib.request import Request, urlopen
from ansible.module_utils.urls import basic_auth_header


class ControllerError(Exception):
    pass


class Client(object):
    def __init__(self, base_url, auth_header, ssl_context, timeout):
        self.base_url = base_url
        self.auth_header = auth_header
        self.ssl_context = ssl_context
        self.timeout = timeout

    def get(self, path):
        request = Request(self.base_url + path)
        request.add_header('Authorization', self.auth_header)
        request.add_header('Accept', 'application/json')
        try:
            response = urlopen(request, timeout=self.timeout, context=self.ssl_context)
        except HTTPError as exc:
            raise ControllerError('HTTP %s for %s' % (exc.code, path))
        except URLError as exc:
            raise ControllerError('%s for %s' % (exc.reason, path))
        try:
            return json.loads(response.read().decode('utf-8'))
        finally:
            response.close()

    def get_all(self, path):
        """Yield every result across a paginated collection."""
        while path:
            payload = self.get(path)
            for result in payload.get('results', []):
                yield result
            path = payload.get('next') or ''


def main():
    module = AnsibleModule(
        argument_spec=dict(
            base_url=dict(type='str', required=True),
            api_base_path=dict(type='str', default='/api/controller/v2'),
            username=dict(type='str', required=True),
            password=dict(type='str', required=True, no_log=True),
            validate_certs=dict(type='bool', default=True),
            organization_id=dict(type='int', required=True),
            created_after=dict(type='str', required=True),
            created_before=dict(type='str', required=True),
            page_size=dict(type='int', default=200),
            threads=dict(type='int', default=16),
            timeout=dict(type='int', default=30),
        ),
        supports_check_mode=True,
    )

    params = module.params
    threads = max(1, min(params['threads'], 64))

    if params['validate_certs']:
        ssl_context = ssl.create_default_context()
    else:
        ssl_context = ssl._create_unverified_context()

    client = Client(
        params['base_url'].rstrip('/'),
        basic_auth_header(params['username'], params['password']),
        ssl_context,
        params['timeout'],
    )

    api = params['api_base_path'].rstrip('/')
    jobs_path = (
        '%s/jobs/?organization=%d&created__gte=%s&created__lte=%s'
        '&page_size=%d&order_by=id'
        % (api, params['organization_id'], params['created_after'],
           params['created_before'], params['page_size'])
    )

    try:
        job_ids = [job['id'] for job in client.get_all(jobs_path)]
    except ControllerError as exc:
        module.fail_json(msg='Could not list this organization\'s jobs: %s' % exc)

    def summaries_for(job_id):
        path = '%s/jobs/%d/job_host_summaries/?page_size=%d&order_by=id' % (
            api, job_id, params['page_size'])
        return [(row.get('host_name'), row.get('host')) for row in client.get_all(path)]

    rows = []
    if job_ids:
        try:
            with ThreadPoolExecutor(max_workers=threads) as pool:
                for result in pool.map(summaries_for, job_ids):
                    rows.extend(result)
        except ControllerError as exc:
            module.fail_json(msg='Could not read job host summaries: %s' % exc)

    # A host name counts as deleted only if every row referencing it has a
    # null host, so a host deleted and later recreated is still billed.
    live = set()
    seen = set()
    for host_name, host_id in rows:
        if not host_name:
            continue
        seen.add(host_name)
        if host_id is not None:
            live.add(host_name)

    module.exit_json(
        changed=False,
        hostnames=sorted(seen),
        deleted_hostnames=sorted(seen - live),
        job_count=len(job_ids),
        summary_count=len(rows),
    )


if __name__ == '__main__':
    main()
