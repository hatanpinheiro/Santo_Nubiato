/**
 * ================================================================
 * ETL Bronze v4.0 — Frontend Application
 * Socket.IO client, file upload, batch processing control
 * ================================================================
 */

(function () {
    'use strict';

    // ── State ──────────────────────────────────────────────────
    const state = {
        socket: null,
        connected: false,
        processing: false,
        uploadedFiles: [],  // [{filename, path, size}, ...]
    };

    // ── DOM References ─────────────────────────────────────────
    const $ = (sel) => document.querySelector(sel);
    const $$ = (sel) => document.querySelectorAll(sel);

    const dom = {
        // Connection
        connectionDot: $('#connection-indicator'),
        connectionText: $('#connection-text'),

        // DB Config
        dbHost: $('#db-host'),
        dbPort: $('#db-port'),
        dbName: $('#db-name'),
        dbUser: $('#db-user'),
        dbPass: $('#db-pass'),
        dbSchema: $('#db-schema'),

        // Upload
        dropZone: $('#drop-zone'),
        fileInput: $('#file-input'),
        fileList: $('#file-list'),
        fileItems: $('#file-items'),
        fileCountBadge: $('#file-count-badge'),
        btnClearFiles: $('#btn-clear-files'),

        // Actions
        btnInfra: $('#btn-infra'),
        btnIngest: $('#btn-ingest'),

        // Console
        consoleContent: $('#console-content'),
        btnCopyLog: $('#btn-copy-log'),
        btnClearLog: $('#btn-clear-log'),

        // Progress
        batchProgress: $('#batch-progress'),
        batchLabel: $('#batch-label'),
        batchFilename: $('#batch-filename'),
        batchProgressFill: $('#batch-progress-fill'),
        layerProgress: $('#layer-progress'),
        layerLabel: $('#layer-label'),
        layerProgressFill: $('#layer-progress-fill'),

        // Toast
        toastContainer: $('#toast-container'),
    };

    // ── Utilities ──────────────────────────────────────────────
    function formatBytes(bytes) {
        if (bytes === 0) return '0 B';
        const k = 1024;
        const sizes = ['B', 'KB', 'MB', 'GB'];
        const i = Math.floor(Math.log(bytes) / Math.log(k));
        return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
    }

    function getDbConfig() {
        return {
            host: dom.dbHost.value.trim(),
            port: dom.dbPort.value.trim(),
            dbname: dom.dbName.value.trim(),
            user: dom.dbUser.value.trim(),
            password: dom.dbPass.value,
            schema: dom.dbSchema.value.trim(),
        };
    }

    // ── Toast System ───────────────────────────────────────────
    function showToast(message, type = 'info', duration = 4000) {
        const icons = {
            success: '✅',
            error: '❌',
            warning: '⚠️',
            info: 'ℹ️',
        };

        const toast = document.createElement('div');
        toast.className = `toast toast-${type}`;
        toast.innerHTML = `
            <span class="toast-icon">${icons[type] || icons.info}</span>
            <span>${message}</span>
        `;

        dom.toastContainer.appendChild(toast);

        setTimeout(() => {
            toast.classList.add('toast-out');
            setTimeout(() => toast.remove(), 300);
        }, duration);
    }

    // ── Socket.IO ──────────────────────────────────────────────
    function initSocket() {
        state.socket = io({
            transports: ['websocket', 'polling'],
            reconnection: true,
            reconnectionAttempts: Infinity,
            reconnectionDelay: 1000,
        });

        state.socket.on('connect', () => {
            state.connected = true;
            updateConnectionUI(true);
        });

        state.socket.on('disconnect', () => {
            state.connected = false;
            updateConnectionUI(false);
        });

        state.socket.on('connection_status', (data) => {
            if (data.status === 'connected') {
                updateConnectionUI(true);
            }
        });

        state.socket.on('log_message', (data) => {
            appendLog(data.message);
        });

        state.socket.on('progress_update', (data) => {
            updateLayerProgress(data.current, data.total);
        });

        state.socket.on('batch_progress', (data) => {
            updateBatchProgress(data.current_file, data.total_files, data.filename);
        });

        state.socket.on('process_started', (data) => {
            state.processing = true;
            setButtonsProcessing(data.type);
            clearConsole();

            if (data.type === 'ingestao') {
                dom.batchProgress.style.display = 'block';
                dom.layerProgress.style.display = 'block';
            } else {
                dom.batchProgress.style.display = 'none';
                dom.layerProgress.style.display = 'none';
            }
        });

        state.socket.on('process_complete', (data) => {
            state.processing = false;
            resetButtons();
            dom.batchProgress.style.display = 'none';
            dom.layerProgress.style.display = 'none';

            const result = data.result || {};
            if (result.success) {
                showToast(result.message || 'Processamento concluído com sucesso!', 'success');
            } else if (result.warning) {
                showToast(result.warning, 'warning');
            } else {
                showToast(result.message || result.error || 'Processamento finalizado com erros.', 'error');
            }
        });

        state.socket.on('process_error', (data) => {
            state.processing = false;
            resetButtons();
            dom.batchProgress.style.display = 'none';
            dom.layerProgress.style.display = 'none';
            showToast(data.error || 'Erro inesperado.', 'error', 6000);
            appendLog(`\n❌ Erro: ${data.error}`);
        });
    }

    function updateConnectionUI(connected) {
        dom.connectionDot.className = `status-dot ${connected ? 'connected' : 'disconnected'}`;
        dom.connectionText.textContent = connected ? 'Conectado' : 'Desconectado';
        updateButtonStates();
    }

    // ── Console ────────────────────────────────────────────────
    function appendLog(message) {
        const line = document.createElement('span');
        line.className = 'log-line';

        // Classify log line for coloring
        if (message.includes('✅')) line.classList.add('log-success');
        else if (message.includes('❌')) line.classList.add('log-error');
        else if (message.includes('⚠️')) line.classList.add('log-warning');
        else if (message.includes('📦') || message.includes('📂')) line.classList.add('log-section');
        else if (message.includes('🔎') || message.includes('🧭') || message.includes('🗺️') || message.includes('🔁')) line.classList.add('log-info');
        else if (message.startsWith('===') || message.startsWith('---')) line.classList.add('log-separator');

        if (message.includes('<img')) {
            line.innerHTML = message;
        } else {
            line.textContent = message;
        }
        dom.consoleContent.appendChild(line);

        // Auto-scroll
        dom.consoleContent.scrollTop = dom.consoleContent.scrollHeight;
    }

    function clearConsole() {
        dom.consoleContent.innerHTML = '';
    }

    function copyLog() {
        const lines = dom.consoleContent.querySelectorAll('.log-line');
        const text = Array.from(lines).map(l => l.textContent).join('\n');
        navigator.clipboard.writeText(text).then(() => {
            showToast('Log copiado para a área de transferência.', 'success', 2000);
        }).catch(() => {
            showToast('Falha ao copiar log.', 'error', 2000);
        });
    }

    // ── Progress ───────────────────────────────────────────────
    function updateBatchProgress(current, total, filename) {
        dom.batchLabel.textContent = `Arquivo ${current}/${total}`;
        dom.batchFilename.textContent = filename || '';
        const pct = total > 0 ? (current / total) * 100 : 0;
        dom.batchProgressFill.style.width = `${pct}%`;
    }

    function updateLayerProgress(current, total) {
        dom.layerLabel.textContent = `Camada ${current}/${total}`;
        const pct = total > 0 ? (current / total) * 100 : 0;
        dom.layerProgressFill.style.width = `${pct}%`;
    }

    // ── File Upload ────────────────────────────────────────────
    function initUpload() {
        // Click to select
        dom.dropZone.addEventListener('click', () => {
            if (!state.processing) dom.fileInput.click();
        });

        // File input change
        dom.fileInput.addEventListener('change', (e) => {
            handleFiles(e.target.files);
            dom.fileInput.value = '';
        });

        // Drag & Drop
        dom.dropZone.addEventListener('dragover', (e) => {
            e.preventDefault();
            dom.dropZone.classList.add('drag-over');
        });

        dom.dropZone.addEventListener('dragleave', (e) => {
            e.preventDefault();
            dom.dropZone.classList.remove('drag-over');
        });

        dom.dropZone.addEventListener('drop', (e) => {
            e.preventDefault();
            dom.dropZone.classList.remove('drag-over');
            handleFiles(e.dataTransfer.files);
        });

        // Clear files
        dom.btnClearFiles.addEventListener('click', () => {
            state.uploadedFiles = [];
            renderFileList();
            updateButtonStates();
        });
    }

    async function handleFiles(fileList) {
        const files = Array.from(fileList).filter(f => f.name.toLowerCase().endsWith('.gpkg'));
        if (files.length === 0) {
            showToast('Selecione apenas arquivos .gpkg', 'warning');
            return;
        }

        // Show upload progress on drop zone
        const overlay = document.createElement('div');
        overlay.className = 'upload-progress-overlay';
        overlay.innerHTML = `
            <div class="upload-spinner"></div>
            <span class="upload-progress-text">Enviando ${files.length} arquivo(s)...</span>
        `;
        dom.dropZone.style.position = 'relative';
        dom.dropZone.appendChild(overlay);

        try {
            const formData = new FormData();
            files.forEach(f => formData.append('files', f));

            const response = await fetch('/api/upload', {
                method: 'POST',
                body: formData,
            });

            const result = await response.json();

            if (result.uploaded && result.uploaded.length > 0) {
                state.uploadedFiles.push(...result.uploaded);
                renderFileList();
                updateButtonStates();
                showToast(`${result.total_uploaded} arquivo(s) enviado(s) com sucesso.`, 'success');
            }

            if (result.errors && result.errors.length > 0) {
                result.errors.forEach(err => showToast(err, 'warning'));
            }
        } catch (err) {
            showToast(`Erro no upload: ${err.message}`, 'error');
        } finally {
            overlay.remove();
        }
    }

    function renderFileList() {
        const files = state.uploadedFiles;
        dom.fileItems.innerHTML = '';

        if (files.length === 0) {
            dom.fileList.style.display = 'none';
            dom.fileCountBadge.style.display = 'none';
            return;
        }

        dom.fileList.style.display = 'block';
        dom.fileCountBadge.style.display = 'inline';
        dom.fileCountBadge.textContent = files.length;

        files.forEach((file, index) => {
            const li = document.createElement('li');
            li.className = 'file-item';
            li.innerHTML = `
                <div class="file-item-icon">
                    <svg width="14" height="14" viewBox="0 0 14 14" fill="none">
                        <path d="M3 1h5l3 3v8a1 1 0 01-1 1H3a1 1 0 01-1-1V2a1 1 0 011-1z" stroke="currentColor" stroke-width="1.2"/>
                        <path d="M8 1v3h3" stroke="currentColor" stroke-width="1.2" stroke-linecap="round"/>
                    </svg>
                </div>
                <div class="file-item-info">
                    <div class="file-item-name" title="${file.filename}">${file.filename}</div>
                    <div class="file-item-size">${formatBytes(file.size)}</div>
                </div>
                <button class="file-item-remove" data-index="${index}" title="Remover">
                    <svg width="12" height="12" viewBox="0 0 12 12" fill="none">
                        <path d="M3 3l6 6M9 3l-6 6" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"/>
                    </svg>
                </button>
            `;
            dom.fileItems.appendChild(li);
        });

        // Remove individual file
        dom.fileItems.querySelectorAll('.file-item-remove').forEach(btn => {
            btn.addEventListener('click', (e) => {
                e.stopPropagation();
                const idx = parseInt(btn.dataset.index, 10);
                state.uploadedFiles.splice(idx, 1);
                renderFileList();
                updateButtonStates();
            });
        });
    }

    // ── Button State Management ────────────────────────────────
    function updateButtonStates() {
        const canRun = state.connected && !state.processing;
        dom.btnInfra.disabled = !canRun;
        dom.btnIngest.disabled = !canRun || state.uploadedFiles.length === 0;
    }

    function setButtonsProcessing(type) {
        dom.btnInfra.disabled = true;
        dom.btnIngest.disabled = true;

        if (type === 'infraestrutura') {
            dom.btnInfra.classList.add('processing');
        } else {
            dom.btnIngest.classList.add('processing');
        }
    }

    function resetButtons() {
        dom.btnInfra.classList.remove('processing');
        dom.btnIngest.classList.remove('processing');
        updateButtonStates();
    }

    // ── Action Handlers ────────────────────────────────────────
    function runInfraestrutura() {
        if (state.processing || !state.connected) return;

        const config = getDbConfig();
        state.socket.emit('run_infraestrutura', config);
    }

    function runIngestao() {
        if (state.processing || !state.connected || state.uploadedFiles.length === 0) return;

        const config = getDbConfig();
        config.files = state.uploadedFiles.map(f => ({
            filename: f.filename,
            path: f.path,
        }));

        state.socket.emit('run_ingestao', config);
    }

    // ── Event Binding ──────────────────────────────────────────
    function bindEvents() {
        dom.btnInfra.addEventListener('click', runInfraestrutura);
        dom.btnIngest.addEventListener('click', runIngestao);
        dom.btnCopyLog.addEventListener('click', copyLog);
        dom.btnClearLog.addEventListener('click', () => {
            clearConsole();
            showToast('Console limpo.', 'info', 1500);
        });
    }

    // ── Init ───────────────────────────────────────────────────
    function init() {
        initSocket();
        initUpload();
        bindEvents();
        updateButtonStates();
    }

    // Start when DOM is ready
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }

})();
