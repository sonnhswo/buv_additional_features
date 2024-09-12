import streamlit as st
from utils import upload_to_blob_storage, processing_uploaded_file, update_bus_schedule_database


# Define valid usernames and passwords
VALID_USERS = {
    "vuongnguyen": "adminvuong",
    "dungnguyen": "admindung",
    "sonnguyen": "Admin123",
    "ngoctran": "Admin123",
}


def authenticate(username, password):
    if username in VALID_USERS and VALID_USERS[username] == password:
        return True
    return False

def login():
    st.sidebar.title("Login")
    username = st.sidebar.text_input("Username")
    password = st.sidebar.text_input("Password", type="password")
    if st.sidebar.button("Login"):
        if authenticate(username, password):
            st.session_state["authenticated"] = True
            st.sidebar.success("Login successful")
            st.rerun()  # Rerun the script to display the main page
        else:
            st.sidebar.error("Invalid username or password")

def logout():
    st.session_state["authenticated"] = False
    
def main():
    if "authenticated" not in st.session_state:
        st.session_state["authenticated"] = False

    if not st.session_state["authenticated"]:
        login()
    else:
        st.sidebar.button("Logout", on_click=logout)
        st.title("Uploading weekly bus schedule")
        uploaded_file = st.file_uploader("Choose a file", type=["xlsx"])
        if uploaded_file is not None:
            with st.spinner("Processing..."):
                # get file name
                filename = uploaded_file.name
                processed_filename = filename.replace(" ", "_")
                # print(filename)

                upload_to_blob_storage(filename=filename, uploaded_file=uploaded_file)

                processing_uploaded_file(processed_filename)
                
                update_bus_schedule_database()
                st.success(f"File '{filename}' uploaded successfully to container!")


if __name__ == "__main__":
    main()