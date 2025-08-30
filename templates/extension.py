import os
import platform

proxies_extension_location = "C:\\Users\\alexo\\Documents\\upwork\\trent lee\\proxies_extension" \
    if platform.system().lower() == "windows" \
    else "proxies_extension"


def proxies(username, password, endpoint, port, directory=proxies_extension_location):
    manifest_json = """
    {
        "version": "1.0.0",
        "manifest_version": 3,
        "name": "MyProxies",
        "permissions": [
            "proxy",
            "tabs",
            "unlimitedStorage",
            "storage",
            "<all_urls>",
            "webRequest",
            "webRequestBlocking"
        ],
        "background": {
            "scripts": ["background.js"]
        },
        "minimum_chrome_version":"22.0.0"
    }
    """

    background_js = """
    var config = {
            mode: "fixed_servers",
            rules: {
              singleProxy: {
                scheme: "http",
                host: "%s",
                port: parseInt(%s)
              },
              bypassList: ["localhost"]
            }
          };

    chrome.proxy.settings.set({value: config, scope: "regular"}, function() {});

    function callbackFn(details) {
        return {
            authCredentials: {
                username: "%s",
                password: "%s"
            }
        };
    }

    chrome.webRequest.onAuthRequired.addListener(
                callbackFn,
                {urls: ["<all_urls>"]},
                ['blocking']
    );
    """ % (
        endpoint,
        port,
        username,
        password,
    )

    # Ensure the directory exists
    os.makedirs(directory, exist_ok=True)

    # Write manifest.json
    manifest_path = os.path.join(directory, "manifest.json")
    with open(manifest_path, "w") as manifest_file:
        manifest_file.write(manifest_json.strip())

    # Write background.js
    background_path = os.path.join(directory, "background.js")
    with open(background_path, "w") as background_file:
        background_file.write(background_js.strip())

    return directory
