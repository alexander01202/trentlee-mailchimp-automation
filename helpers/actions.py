from time import sleep
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from bs4 import BeautifulSoup
import random


def get_element_attribute(driver_arg, xpath, attribute):
    try:
        element = WebDriverWait(driver_arg, 10).until(
            EC.presence_of_element_located((By.XPATH, xpath))
        )
        link = element.get_attribute(attribute)
        return link
    except Exception as err:
        print(err)
        return ''


def move_to_element(driver_arg, xpath):
    try:
        actions = ActionChains(driver_arg)
        element = WebDriverWait(driver_arg, 10).until(
            EC.presence_of_element_located((By.XPATH, xpath))
        )
        actions.move_to_element(element).perform()
    except:
        pass


def move_to_custom_element(driver_arg, element):
    try:
        actions = ActionChains(driver_arg)
        actions.move_to_element(element).perform()
    except:
        pass


def move_mouse_randomly(driver_arg, xoffset=[-100, 100], yoffset=[-100, 100] ):
    try:
        actions = ActionChains(driver_arg)
        actions.move_by_offset(random.randint(*xoffset), random.randint(*yoffset)).perform()
    except Exception as err:
        print("ERROR MOVING ELEMENT RANDOMLY ==> ", {err})
        pass


def scroll_to_element(driver_arg, xpath):
    try:
        element = WebDriverWait(driver_arg, 10).until(
            EC.presence_of_element_located((By.XPATH, xpath))
        )
        driver_arg.execute_script("arguments[0].scrollIntoView(true);", element)
    except:
        pass


def get_element_text(driver_arg, xpath):
    try:
        element = WebDriverWait(driver_arg, 10).until(
            EC.presence_of_element_located((By.XPATH, xpath))
        )
        return element.text
    except:
        pass


def get_element_with_soup(soup: BeautifulSoup, selector):
    try:
        return soup.select(selector)
    except:
        pass


def get_element_text_with_soup(soup: BeautifulSoup, selector):
    try:
        element = soup.select_one(selector)
        return element.get_text(strip=True) if element else None
    except:
        pass


def wait_for_element(driver_arg, xpath, wait=10):
    try:
        WebDriverWait(driver_arg, wait).until(
            EC.presence_of_element_located((By.XPATH, xpath))
        )
    except:
        pass


def wait_for_elements(driver_arg, xpath):
    try:
        WebDriverWait(driver_arg, 10).until(
            EC.presence_of_all_elements_located((By.XPATH, xpath))
        )
    except:
        pass


# def human_clicker_click_by_id(driver_arg, id):
#     try:
#         element = WebDriverWait(driver_arg, 10).until(
#             EC.presence_of_element_located((By.ID, id))
#         )
#         driver_arg.execute_script("arguments[0].click();", element)
#     except:
#         pass

#
# def human_clicker_js3(driver_arg, xpath, index):
#     try:
#         element = WebDriverWait(driver_arg, 10).until(
#             EC.presence_of_all_elements_located((By.XPATH, xpath))
#         )
#         driver_arg.execute_script("arguments[0].click();", element[index])
#     except:
#         pass


def get_element(driver_arg, xpath):
    try:
        element = WebDriverWait(driver_arg, 10).until(
            EC.presence_of_all_elements_located((By.XPATH, xpath))
        )
        return element
    except:
        return ''


def human_clicker_js_single_el(driver_arg, xpath):
    try:
        element = WebDriverWait(driver_arg, 10).until(
            EC.presence_of_all_elements_located((By.XPATH, xpath))
        )
        randomIndex = random.randrange(len(element))
        driver_arg.execute_script("arguments[0].click();", element[randomIndex])
    except:
        pass


def get_element_by_js_path(driver_arg, js_path: str):
    try:
        element = driver_arg.execute_script(f"return {js_path}")
        return element if element else None
    except Exception as err:
        return ''


def human_clicker_by_js_path(driver_arg, js_path: str, print_error=False):
    try:
        element = driver_arg.execute_script(f"return {js_path}")
        if not element:
            print("element not found")
            return None
        driver_arg.execute_script("arguments[0].click();", element)

    except Exception as err:
        # if print_error:
        print("ERROR ==> ", err)
        pass

def human_typer(driver_arg, xpath, text: str):
    print("TYPING VALUE")
    try:
        element = WebDriverWait(driver_arg, 20).until(
            EC.presence_of_element_located((By.XPATH, xpath))
        )
        for s in text:
            element.send_keys(s)
            sleep(random.uniform(0.07, 0.82))
    except Exception as err:
        print("TYPING VALUE ==> ", err)
        pass


def change_input_value(driver_arg, xpath, input_value):
    try:
        input_field = WebDriverWait(driver_arg, 10).until(
            EC.presence_of_element_located((By.XPATH, xpath))
        )
        driver_arg.execute_script("arguments[0].value = arguments[1];", input_field, input_value)
    except:
        pass


def human_clicker_js(driver_arg, xpath):
    try:
        element = WebDriverWait(driver_arg, 20).until(
            EC.presence_of_element_located((By.XPATH, xpath))
        )
        element.click()
        # move_to_element(driver_arg, xpath)
        # sleep(random.uniform(0.67, 1.12))
        # driver_arg.execute_script("arguments[0].click();", element)
        return True
    except Exception as err:
        print(f"{xpath} not clicking... {err}")
        try:
            driver_arg.execute_script("arguments[0].click();", element)
        except:
            pass
        return False


def random_wait(lower_limit, uper_limit):
    try:
        time_wait = random.randint(lower_limit, uper_limit)
        sleep(time_wait)
    except:
        pass


def send_keys_interval(el, string):
    try:
        for char in string:
            el.send_keys(char)
            rand_wait = random.uniform(0.04, 0.1)
            sleep(rand_wait)
    except:
        pass
