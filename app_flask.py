from flask import Flask, render_template, request, redirect, url_for, send_from_directory, jsonify, session, g

app = Flask(__name__)
app.secret_key = '1234'

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/loguin', methods=['POST'])
def loguin():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        if username == 'admin' and password == 'admin':
            session['username'] = username
            return redirect(url_for('home'))
        else:
            return jsonify({'error': 'Invalid credentials'}), 401

@app.route('/home')
def home():
    if 'username' in session:
        return render_template('home.html', username=session['username'])
    return redirect(url_for('index'))
