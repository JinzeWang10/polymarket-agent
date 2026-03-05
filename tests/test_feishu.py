import httpx
import respx

from src.alerts.feishu import FeishuAlerter


@respx.mock
def test_send_card_success():
    url = "https://open.feishu.cn/open-apis/bot/v2/hook/test"
    respx.post(url).mock(return_value=httpx.Response(200, json={"code": 0}))
    alerter = FeishuAlerter(url, httpx.Client())
    assert alerter.send_card({"header": {}, "elements": []}) is True


@respx.mock
def test_send_card_api_error():
    url = "https://open.feishu.cn/open-apis/bot/v2/hook/test"
    respx.post(url).mock(
        return_value=httpx.Response(200, json={"code": 9499, "msg": "bad request"})
    )
    alerter = FeishuAlerter(url, httpx.Client())
    assert alerter.send_card({"header": {}, "elements": []}) is False


def test_send_card_no_url():
    alerter = FeishuAlerter("", httpx.Client())
    assert alerter.send_card({"header": {}, "elements": []}) is False
