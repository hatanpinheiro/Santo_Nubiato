"""
================================================================================
PROJETO: Automacao de Carga - Base BRONZE (Web Edition)
SERVER: Flask + Socket.IO - Interface web para ingestao de dados
VERSAO: 4.0.0-web
================================================================================
"""

import os
import uuid
import threading
from flask import Flask, render_template, request, jsonify
from flask_socketio import SocketIO, emit
from etl_engine import ETLEngine

app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'etl-bronze-secret-key')
app.config['MAX_CONTENT_LENGTH'] = 2 * 1024 * 1024 * 1024  # 2 GB
app.config['UPLOAD_FOLDER'] = os.environ.get('UPLOAD_FOLDER', '/app/uploads')

socketio = SocketIO(app, async_mode='threading', max_http_buffer_size=2 * 1024 * 1024 * 1024)

# Estado global do processamento
processing_state = {
    "running": False,
    "type": None,
    "progress": 0,
    "total": 0,
}

active_engines = {}



def ensure_upload_dir():
    os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/api/upload', methods=['POST'])
def upload_files():
    """Upload de um ou vários arquivos GPKG."""
    ensure_upload_dir()

    if 'files' not in request.files:
        return jsonify({"success": False, "error": "Nenhum arquivo enviado."}), 400

    files = request.files.getlist('files')
    if not files or all(f.filename == '' for f in files):
        return jsonify({"success": False, "error": "Nenhum arquivo selecionado."}), 400

    uploaded = []
    errors = []

    for f in files:
        if f.filename == '':
            continue
        if not f.filename.lower().endswith('.gpkg'):
            errors.append(f"{f.filename}: formato inválido (somente .gpkg)")
            continue

        # Gera nome seguro mantendo o nome original para extração de metadados
        safe_dir = os.path.join(app.config['UPLOAD_FOLDER'], uuid.uuid4().hex[:8])
        os.makedirs(safe_dir, exist_ok=True)
        filepath = os.path.join(safe_dir, f.filename)
        f.save(filepath)
        uploaded.append({
            "filename": f.filename,
            "path": filepath,
            "size": os.path.getsize(filepath)
        })

    return jsonify({
        "success": len(uploaded) > 0,
        "uploaded": uploaded,
        "errors": errors,
        "total_uploaded": len(uploaded),
        "total_errors": len(errors)
    })


@app.route('/api/uploaded-files', methods=['GET'])
def list_uploaded_files():
    """Lista arquivos GPKG já carregados no servidor."""
    ensure_upload_dir()
    files = []
    for root, dirs, filenames in os.walk(app.config['UPLOAD_FOLDER']):
        for fname in filenames:
            if fname.lower().endswith('.gpkg'):
                fpath = os.path.join(root, fname)
                files.append({
                    "filename": fname,
                    "path": fpath,
                    "size": os.path.getsize(fpath)
                })
    return jsonify({"files": files})


@app.route('/api/status', methods=['GET'])
def get_status():
    """Retorna o estado atual do processamento."""
    return jsonify(processing_state)


@app.route('/api/ping_db', methods=['POST'])
def ping_db():
    import time, psycopg2
    data = request.json
    if not data:
        return jsonify({"success": False, "error": "No data"}), 400
    try:
        start = time.time()
        conn = psycopg2.connect(
            host=data.get('host', ''), 
            port=data.get('port', ''), 
            dbname=data.get('dbname', ''), 
            user=data.get('user', ''), 
            password=data.get('password', ''), 
            connect_timeout=2
        )
        conn.close()
        latency = int((time.time() - start) * 1000)
        return jsonify({"success": True, "latency": latency})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})


def create_emitters(sid):
    """Cria funções de callback de log e progresso vinculadas a um socket."""
    import time

    def emit_log(msg):
        socketio.emit('log_message', {'message': msg}, room=sid)
        time.sleep(0.01)  # Pequeno yield para permitir envio via threading

    def emit_progress(current, total):
        processing_state["progress"] = current
        processing_state["total"] = total
        socketio.emit('progress_update', {'current': current, 'total': total}, room=sid)
        time.sleep(0.01)

    return emit_log, emit_progress


def parse_db_config(data):
    """Extrai configuração do banco a partir dos dados do formulário."""
    return {
        "host": data.get("host", "localhost").strip(),
        "port": data.get("port", "5432").strip(),
        "dbname": data.get("dbname", "bronze").strip(),
        "user": data.get("user", "postgres").strip(),
        "password": data.get("password", "").strip(),
    }


@socketio.on('run_infraestrutura')
def handle_infraestrutura(data):
    """Executa criação/verificação da infraestrutura BRONZE."""
    sid = request.sid

    if processing_state["running"]:
        emit('process_error', {'error': 'Já existe um processamento em andamento.'}, room=sid)
        return

    processing_state["running"] = True
    processing_state["type"] = "infraestrutura"
    emit('process_started', {'type': 'infraestrutura'}, room=sid)

    def run():
        try:
            emit_log, emit_progress = create_emitters(sid)
            engine = ETLEngine(emit_log=emit_log, emit_progress=emit_progress)
            active_engines[sid] = engine

            db_config = parse_db_config(data)
            schema = data.get("schema", "").strip()

            result = engine.executar_infraestrutura(db_config, schema)
            socketio.emit('process_complete', {
                'type': 'infraestrutura',
                'result': result
            }, room=sid)
        except Exception as e:
            socketio.emit('process_error', {'error': str(e)}, room=sid)
        finally:
            active_engines.pop(sid, None)
            processing_state["running"] = False
            processing_state["type"] = None

    socketio.start_background_task(run)


@socketio.on('run_ingestao')
def handle_ingestao(data):
    """Executa a ingestão de dados para um ou vários GPKGs."""
    sid = request.sid

    if processing_state["running"]:
        emit('process_error', {'error': 'Já existe um processamento em andamento.'}, room=sid)
        return

    processing_state["running"] = True
    processing_state["type"] = "ingestao"
    emit('process_started', {'type': 'ingestao'}, room=sid)

    def run():
        try:
            emit_log, emit_progress = create_emitters(sid)
            engine = ETLEngine(emit_log=emit_log, emit_progress=emit_progress)
            active_engines[sid] = engine

            db_config = parse_db_config(data)
            schema = data.get("schema", "").strip()
            gpkg_files = data.get("files", [])

            if not gpkg_files:
                emit_log("❌ Nenhum arquivo GPKG selecionado para ingestão.")
                socketio.emit('process_complete', {
                    'type': 'ingestao',
                    'result': {"success": False, "error": "Nenhum arquivo GPKG selecionado."}
                }, room=sid)
                return

            total_files = len(gpkg_files)
            resumo_geral = {"concluidos": 0, "com_erro": 0, "total": total_files}
            arquivos_processados = []

            for idx, gpkg_info in enumerate(gpkg_files, 1):
                gpkg_path = gpkg_info.get("path", "")
                gpkg_name = gpkg_info.get("filename", os.path.basename(gpkg_path))

                emit_log(f"\n{'='*70}")
                emit_log(f"📂 ARQUIVO {idx}/{total_files}: {gpkg_name}")
                emit_log(f"{'='*70}")

                # Atualiza progresso geral
                socketio.emit('batch_progress', {
                    'current_file': idx,
                    'total_files': total_files,
                    'filename': gpkg_name
                }, room=sid)
                import time; time.sleep(0.01)

                result = engine.executar_ingestao(db_config, schema, gpkg_path)

                sucesso = result.get("success", False)
                if sucesso:
                    resumo_geral["concluidos"] += 1
                else:
                    resumo_geral["com_erro"] += 1

                arquivos_processados.append({
                    "filename": gpkg_name,
                    "sucesso": sucesso
                })

            emit_log(f"\n{'='*70}")
            emit_log("📊 RESUMO GERAL DO LOTE")
            emit_log(f"{'='*70}")
            emit_log(f"Total de arquivos: {resumo_geral['total']}")
            emit_log(f"Concluídos com sucesso: {resumo_geral['concluidos']}")
            emit_log(f"Com erro: {resumo_geral['com_erro']}")
            emit_log("\nLista de arquivos")
            for arq in arquivos_processados:
                status_icon = "✅" if arq["sucesso"] else "❌"
                emit_log(f"  {status_icon} {arq['filename']}")
                
            if resumo_geral["com_erro"] == 0:
                emit_log("\nAmém.<br><br><img src='/static/img/logo.png' style='width: 60px; height: 60px; object-fit: contain; border-radius: 4px;'>")

            socketio.emit('process_complete', {
                'type': 'ingestao',
                'result': {
                    "success": resumo_geral["com_erro"] == 0,
                    "resumo_geral": resumo_geral,
                    "message": "Lote processado com sucesso." if resumo_geral["com_erro"] == 0 else "Lote finalizado com erros."
                }
            }, room=sid)

        except Exception as e:
            socketio.emit('process_error', {'error': str(e)}, room=sid)
        finally:
            active_engines.pop(sid, None)
            processing_state["running"] = False
            processing_state["type"] = None

    socketio.start_background_task(run)


@socketio.on('connect')
def handle_connect():
    emit('connection_status', {'status': 'connected'})


@socketio.on('disconnect')
def handle_disconnect():
    sid = request.sid
    engine = active_engines.get(sid)
    if engine:
        engine.parar_ingestao()


@socketio.on('pause_process')
def handle_pause():
    sid = request.sid
    engine = active_engines.get(sid)
    if engine:
        engine.pausar_ingestao()


@socketio.on('resume_process')
def handle_resume():
    sid = request.sid
    engine = active_engines.get(sid)
    if engine:
        engine.retomar_ingestao()


@socketio.on('stop_process')
def handle_stop():
    sid = request.sid
    engine = active_engines.get(sid)
    if engine:
        engine.parar_ingestao()


@socketio.on('restart_server')
def handle_restart_server():
    sid = request.sid
    engine = active_engines.get(sid)
    if engine:
        engine.parar_ingestao()
    
    import threading
    def restart():
        import time
        import os
        time.sleep(1)
        os._exit(1)
        
    threading.Thread(target=restart).start()


if __name__ == '__main__':
    ensure_upload_dir()
    socketio.run(app, host='0.0.0.0', port=5000, debug=False, allow_unsafe_werkzeug=True)
