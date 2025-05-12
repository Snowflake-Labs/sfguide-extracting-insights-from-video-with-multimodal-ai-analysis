import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.cortex import Complete
import _snowflake
import json

# Write directly to the app
st.set_page_config(layout="wide", initial_sidebar_state="expanded")
st.title("Meeting analysis")
st.caption(
    f"""This application lets you analyze the video, audio transcript, 
    and on-screen content of meetings.
    """
)

# Get the current credentials
session = get_active_session()

# Constants
CHAT_MEMORY = 20
MODEL = "claude-3-5-sonnet"
SEMANTIC_MODEL_FILE = "@hol_db.public.model/meeting_analysis.yaml"

# Reset chat conversation
def reset_conversation():
    st.session_state.messages = [
        {
            "role": "assistant",
            "content": "What question do you have about the meeting?",
        }
    ]


with st.expander(":pencil: Meeting part"):
  meeting = st.selectbox(
    "Pick meeting part to analyze",
    [
        "IS1004c"
    ],
)
st.button("Reset Chat", on_click=reset_conversation)

def get_analyst_response(chat, meeting):
    chat_summary = Complete(
        MODEL,
        "Provide the most recent question with essential context from this chat: "
        + chat
    ).replace("'", "")

    request_body = {
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": f"""Answer regarding meeting part {meeting}: {chat_summary}"""
                    }
                ]
            }
        ],
        "semantic_model_file": SEMANTIC_MODEL_FILE,
    }

    resp = _snowflake.send_snow_api_request(
        "POST",  # method
        "/api/v2/cortex/analyst/message",  # path
        {},  # headers
        {},  # params
        request_body,  # body
        None,  # request_guid
        50000,  # timeout in milliseconds
    )

    parsed_content = json.loads(resp["content"])

    # Check if the response is successful
    if resp["status"] < 400:
        # Return the content of the response as a JSON object
        return parsed_content, None
    else:
        # Craft readable error message
        error_msg = f"""
🚨 An Analyst API error has occurred 🚨

* response code: `{resp['status']}`
* request-id: `{parsed_content['request_id']}`
* error code: `{parsed_content['error_code']}`

Message:
```
{parsed_content['message']}
```
        """

    # Can set `st.info("Selected Source:" + ...)
    return parsed_content, error_msg

def get_query_exec_result(query: str):
    global session
    try:
        df = session.sql(query).to_pandas()
        return df, None
    except SnowparkSQLException as e:
        return None, str(e)


def parse_analyst_response(json_data):
    # Access the content array inside message
    content_array = json_data.get('message', {}).get('content', [])
    
    sql = message = suggestions = None
    # Check if any element has type 'sql'
    for item in content_array:
        if item.get('type') == 'sql':
            sql = item.get('statement')
        elif item.get('type') == 'text':
            message = item.get('text');
        elif item.get('type') == 'suggestions':
            suggestions = item.get('suggestions')

    if sql != None:
        return sql, None, None;
    else:
        return None, message, suggestions

    
def get_prompt(chat, context):
    prompt = f"""Answer this new user question about the content of the meeting 
        that took place. Be concise and only answer the latest question.
        The question is in the chat.
        Chat: <chat> {chat} </chat>.
        Context: <context> {context} </context>."""
    return prompt.replace("'", "")


if "messages" not in st.session_state:
    reset_conversation()

if user_message := st.chat_input():
    st.session_state.messages.append({"role": "user", "content": user_message})

for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

if st.session_state.messages[-1]["role"] != "assistant":
    chat = str(st.session_state.messages[-CHAT_MEMORY:]).replace("'", "")
    with st.chat_message("assistant"):
        with st.status("Answering..", expanded=True) as status:
            st.write("Finding relevant meeting information...")
            analyst_response, error_msg = get_analyst_response(chat, meeting)
            if error_msg is None:
                sql, message, suggestions = parse_analyst_response(analyst_response)
                if sql:
                    st.write("Using semantic query to answer your question...")          
                    st.code(sql, language="sql", wrap_lines=True)
                    result, error = get_query_exec_result(sql)
                    if error == None:
                        st.write("Result...")
                        st.write(result)
                        prompt = get_prompt(chat, result.iloc[:, 0].tolist())
                        response = Complete(MODEL, prompt)
                    else:
                        st.write(error)
                else:
                    st.write("Couldn't match semantic query") 
                    response = message
                status.update(label="Complete!", state="complete", expanded=False)
            else:
                st.write(error_msg);
                response = "Could not answer this due to an error."
                status.update(label="Error", state="error")
        st.markdown(response)
        if suggestions != None:
            for suggestion_index, suggestion in enumerate(suggestions):
                st.button(
                    suggestion, 
                    on_click=lambda: st.session_state.messages.append({"role": "user", "content": suggestion})
                )
    st.session_state.messages.append({"role": "assistant", "content": response})