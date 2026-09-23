import uuid

import responses
from dagster_powerbi import PowerBIWorkspace
from dagster_powerbi.resource import (
    BASE_API_URL,
    MICROSOFT_LOGIN_URL,
    PowerBIServicePrincipal,
    PowerBIToken,
)


@responses.activate
def test_basic_resource_request() -> None:
    fake_token = uuid.uuid4().hex
    fake_workspace_id = uuid.uuid4().hex
    resource = PowerBIWorkspace(
        credentials=PowerBIToken(api_token=fake_token),
        workspace_id=fake_workspace_id,
    )

    responses.add(
        method=responses.GET,
        url=f"{BASE_API_URL}/groups/{fake_workspace_id}/reports",
        json={},
        status=200,
    )

    resource._get_reports()  # noqa: SLF001

    assert len(responses.calls) == 1
    assert responses.calls[0].request.headers["Authorization"] == f"Bearer {fake_token}"


@responses.activate
def test_service_principal_auth() -> None:
    fake_token = uuid.uuid4().hex
    fake_workspace_id = uuid.uuid4().hex
    fake_client_id = uuid.uuid4().hex
    fake_client_secret = uuid.uuid4().hex
    fake_tenant_id = uuid.uuid4().hex

    resource = PowerBIWorkspace(
        credentials=PowerBIServicePrincipal(
            client_id=fake_client_id,
            client_secret=fake_client_secret,
            tenant_id=fake_tenant_id,
        ),
        workspace_id=fake_workspace_id,
    )

    responses.add(
        method=responses.POST,
        url=MICROSOFT_LOGIN_URL.format(tenant_id=fake_tenant_id),
        json={"access_token": fake_token},
        status=200,
    )

    responses.add(
        method=responses.GET,
        url=f"{BASE_API_URL}/groups/{fake_workspace_id}/reports",
        json={},
        status=200,
    )

    resource._get_reports()  # noqa: SLF001

    assert len(responses.calls) == 2
    assert fake_client_id in responses.calls[0].request.body
    assert fake_client_secret in responses.calls[0].request.body
    assert responses.calls[1].request.headers["Authorization"] == f"Bearer {fake_token}"


@responses.activate
def test_trigger_refresh_returns_request_id() -> None:
    fake_token = uuid.uuid4().hex
    fake_workspace_id = uuid.uuid4().hex
    fake_dataset_id = uuid.uuid4().hex
    fake_request_id = str(uuid.uuid4())

    resource = PowerBIWorkspace(
        credentials=PowerBIToken(api_token=fake_token),
        workspace_id=fake_workspace_id,
    )

    responses.add(
        method=responses.POST,
        url=f"{BASE_API_URL}/groups/{fake_workspace_id}/datasets/{fake_dataset_id}/refreshes",
        status=202,
        headers={"x-ms-request-id": fake_request_id},
    )

    assert resource.trigger_refresh(fake_dataset_id) == fake_request_id


@responses.activate
def test_poll_refresh_uses_request_id_to_avoid_race_condition() -> None:
    """Regression test: poll_refresh must check the status of the refresh identified
    by request_id, not just the most recently started refresh for the dataset, which
    may belong to a concurrently triggered refresh.
    """
    fake_token = uuid.uuid4().hex
    fake_workspace_id = uuid.uuid4().hex
    fake_dataset_id = uuid.uuid4().hex
    fake_request_id = str(uuid.uuid4())

    resource = PowerBIWorkspace(
        credentials=PowerBIToken(api_token=fake_token),
        workspace_id=fake_workspace_id,
        refresh_poll_interval=0,
    )

    # The dataset's most recent refresh (e.g. started by another concurrent process)
    # is still unfinished, while the refresh we triggered has already completed. If
    # poll_refresh ignored request_id and looked at the latest refresh in the list, it
    # would spin forever/time out instead of observing our refresh's completion.
    responses.add(
        method=responses.GET,
        url=f"{BASE_API_URL}/groups/{fake_workspace_id}/datasets/{fake_dataset_id}/refreshes",
        json={"value": [{"status": "Unknown"}]},
        status=200,
    )
    responses.add(
        method=responses.GET,
        url=f"{BASE_API_URL}/groups/{fake_workspace_id}/datasets/{fake_dataset_id}/refreshes/{fake_request_id}",
        json={"status": "Completed"},
        status=200,
    )

    resource.poll_refresh(fake_dataset_id, request_id=fake_request_id)

    assert len(responses.calls) == 1
    assert responses.calls[0].request.url == (
        f"{BASE_API_URL}/groups/{fake_workspace_id}/datasets/{fake_dataset_id}/refreshes/{fake_request_id}"
    )
