import os
import sys
import socket
import threading
import time
import webview
from app import app, socketio, ensure_upload_dir

def get_free_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('127.0.0.1', 0))
    port = s.getsockname()[1]
    s.close()
    return port

def start_server(port):
    ensure_upload_dir()
    # Inicia o servidor do Flask-SocketIO
    socketio.run(app, host='127.0.0.1', port=port, debug=False, allow_unsafe_werkzeug=True)

if __name__ == '__main__':
    # Se estiver rodando como bundle do PyInstaller, define os paths corretos
    if getattr(sys, 'frozen', False):
        application_path = sys._MEIPASS
        # Templates e static devem ser lidos do MEIPASS
        app.template_folder = os.path.join(application_path, 'templates')
        app.static_folder = os.path.join(application_path, 'static')
        
        # Uploads deve ir para uma pasta com permissão de escrita no sistema
        app.config['UPLOAD_FOLDER'] = os.path.join(os.path.expanduser('~'), 'ETL_RF_Uploads')
    else:
        application_path = os.path.dirname(os.path.abspath(__file__))
        app.config['UPLOAD_FOLDER'] = os.path.join(application_path, 'uploads')

    port = get_free_port()
    
    server_thread = threading.Thread(target=start_server, args=(port,), daemon=True)
    server_thread.start()
    
    # Aguarda o servidor iniciar
    time.sleep(1)

    try:
        window = webview.create_window(
            title='ETL RF - Base Bronze (Desktop)',
            url=f'http://127.0.0.1:{port}',
            width=1024,
            height=768,
            min_size=(800, 600)
        )
        
        # Inicia a interface gráfica
        webview.start()
    except Exception as e:
        print(f"Erro ao iniciar interface nativa: {e}")
        print("Iniciando via navegador padrão...")
        import webbrowser
        webbrowser.open(f'http://127.0.0.1:{port}')
        
        # Mantém o servidor rodando enquanto a janela do console estiver aberta
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            pass
    
    # Encerra o programa quando a janela fechar
    os._exit(0)
