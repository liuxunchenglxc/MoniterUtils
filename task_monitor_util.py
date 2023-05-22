import streamlit as st
import subprocess


def subprocess_getoutput(stmt):
    result = subprocess.getoutput(stmt)
    return result


st.title('Task Status Monitor')
st.subheader('GPU Status')
####################################
#
# Use 'which gpustat' to get the path of gpustat
#
####################################
gpustat_path = '/the_path_to/gpustat'
st.text(subprocess_getoutput(gpustat_path + ' --no-color --no-header'))
st.subheader('Task Status')
####################################
#
# Some command of showing task status
#
####################################
cmd = 'the_command of showing your task status'
st.text(subprocess_getoutput(cmd))
####################################
#
# The code is simple, so, you can extend it.
#
####################################
more_cmd = 'another command you want'
st.text(subprocess_getoutput(more_cmd))
####################################
#
# Actually, you can plot some figures with streamlit.
# If you want, access: https://docs.streamlit.io/library/api-reference/charts
#
####################################
