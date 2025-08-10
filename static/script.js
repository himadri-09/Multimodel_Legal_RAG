// Updated script.js with multi-file upload and caching support
let selectedFiles = [];
let uploadedPDFs = new Set();

// Drag and drop functionality
const uploadArea = document.getElementById('upload-area');
const fileInput = document.getElementById('pdf-upload');

uploadArea.addEventListener('dragover', (e) => {
  e.preventDefault();
  uploadArea.classList.add('dragover');
});

uploadArea.addEventListener('dragleave', () => {
  uploadArea.classList.remove('dragover');
});

uploadArea.addEventListener('drop', (e) => {
  e.preventDefault();
  uploadArea.classList.remove('dragover');
  const files = Array.from(e.dataTransfer.files).filter(file => file.type === 'application/pdf');
  handleFileSelection(files);
});

fileInput.addEventListener('change', (e) => {
  const files = Array.from(e.target.files);
  handleFileSelection(files);
});

function handleFileSelection(files) {
  selectedFiles = files;
  displayFileList();
  document.getElementById('upload-btn').disabled = files.length === 0;
}

function displayFileList() {
  const fileList = document.getElementById('file-list');
  const uploadBtn = document.getElementById('upload-btn');
  
  if (selectedFiles.length === 0) {
    fileList.style.display = 'none';
    uploadBtn.disabled = true;
    return;
  }

  fileList.style.display = 'block';
  fileList.innerHTML = '';

  selectedFiles.forEach((file, index) => {
    const fileName = file.name.replace('.pdf', '');
    const isUploaded = uploadedPDFs.has(fileName);
    
    const fileItem = document.createElement('div');
    fileItem.className = 'file-item';
    fileItem.innerHTML = `
      <div class="file-info">
        <span>📄</span>
        <span>${file.name}</span>
        <span class="file-status ${isUploaded ? 'status-cached' : 'status-new'}">
          ${isUploaded ? '✓ Cached' : 'New'}
        </span>
      </div>
      <div style="color: #6b7280; font-size: 0.9em;">
        ${(file.size / 1024 / 1024).toFixed(1)} MB
      </div>
    `;
    fileList.appendChild(fileItem);
  });

  uploadBtn.disabled = false;
}

async function uploadPDFs() {
  if (selectedFiles.length === 0) return;

  const uploadBtn = document.getElementById('upload-btn');
  const progressBar = document.getElementById('progress-bar');
  const progressFill = document.getElementById('progress-fill');
  const fileList = document.getElementById('file-list');
  const pdfSelect = document.getElementById('pdf-select');
  const stats = document.getElementById('upload-stats');

  uploadBtn.disabled = true;
  uploadBtn.textContent = '⏳ Processing...';
  progressBar.style.display = 'block';
  stats.style.display = 'grid';

  let totalFiles = selectedFiles.length;
  let cachedFiles = 0;
  let processedFiles = 0;
  let totalTimeSaved = 0;

  // Update stats display
  document.getElementById('total-files').textContent = totalFiles;

  for (let i = 0; i < selectedFiles.length; i++) {
    const file = selectedFiles[i];
    const fileName = file.name.replace('.pdf', '');
    
    // Update progress
    const progress = ((i + 1) / totalFiles) * 100;
    progressFill.style.width = `${progress}%`;

    // Update file status in list
    const fileItems = fileList.querySelectorAll('.file-item');
    const currentItem = fileItems[i];
    const statusElement = currentItem.querySelector('.file-status');
    statusElement.textContent = 'Processing...';
    statusElement.className = 'file-status status-processing';

    try {
      const formData = new FormData();
      formData.append('file', file);

      const response = await fetch('/upload', {
        method: 'POST',
        body: formData,
      });

      const data = await response.json();

      if (response.ok) {
        uploadedPDFs.add(data.pdf_name);
        
        // Update dropdown if not already present
        if (!Array.from(pdfSelect.options).some(opt => opt.value === data.pdf_name)) {
          const option = document.createElement('option');
          option.value = data.pdf_name;
          option.textContent = data.pdf_name;
          pdfSelect.appendChild(option);
        }

        // Update status and stats
        if (data.cached) {
          cachedFiles++;
          statusElement.textContent = '⚡ From Cache';
          statusElement.className = 'file-status status-cached';
          if (data.processing_time_saved) {
            const timeSaved = parseInt(data.processing_time_saved.match(/\d+/)[0]);
            totalTimeSaved += timeSaved;
          }
        } else {
          processedFiles++;
          statusElement.textContent = '✅ Processed';
          statusElement.className = 'file-status status-complete';
        }

        // Update live stats
        document.getElementById('cached-files').textContent = cachedFiles;
        document.getElementById('processed-files').textContent = processedFiles;
        document.getElementById('time-saved').textContent = `${totalTimeSaved}s`;

      } else {
        statusElement.textContent = '❌ Error';
        statusElement.className = 'file-status status-error';
        console.error('Upload error:', data.error);
      }
    } catch (error) {
      statusElement.textContent = '❌ Network Error';
      statusElement.className = 'file-status status-error';
      console.error('Network error:', error);
    }
  }

  // Reset UI
  uploadBtn.disabled = false;
  uploadBtn.textContent = '📤 Process Selected Files';
  progressBar.style.display = 'none';
  selectedFiles = [];
  fileInput.value = '';
  
  // Show final summary for a few seconds then hide file list
  setTimeout(() => {
    fileList.style.display = 'none';
  }, 3000);
}

async function askQuestion() {
  const query = document.getElementById('query-input').value.trim();
  const pdfName = document.getElementById('pdf-select').value;
  const answerArea = document.getElementById('answer-area');
  const imagesGrid = document.getElementById('images-grid');

  if (!query) {
    alert('Please enter a question.');
    return;
  }

  // Show thinking state
  answerArea.innerHTML = `
    <div class="thinking">
      <div class="spinner"></div>
      <span>Analyzing your question and searching through documents...</span>
    </div>
  `;
  imagesGrid.innerHTML = '';

  try {
    const response = await fetch('/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ query, pdf_name: pdfName }),
    });

    const data = await response.json();

    if (response.ok) {
      // Display answer
      answerArea.innerHTML = `
        <div style="line-height: 1.6;">
          <strong style="color: #1f2937; font-size: 1.1em;">Answer:</strong>
          <div style="margin-top: 15px; color: #374151;">
            ${data.answer.replace(/\n/g, '<br>')}
          </div>
        </div>
      `;

      // Display images if any
      if (data.images && data.images.length > 0) {
        imagesGrid.innerHTML = '';
        data.images.forEach(img => {
          const imageItem = document.createElement('div');
          imageItem.className = 'image-item';
          imageItem.innerHTML = `
            <img src="${img.url}" alt="${img.caption}" />
            <div class="image-caption">
              <div style="display: flex; align-items: center; justify-content: space-between; margin-bottom: 10px;">
                <span class="page-badge">Page ${img.page}</span>
              </div>
              <div style="color: #6b7280; font-size: 0.9em;">
                ${img.caption}
              </div>
            </div>
          `;
          imagesGrid.appendChild(imageItem);
        });
      }
    } else {
      answerArea.innerHTML = `
        <div style="color: #ef4444; padding: 20px; text-align: center;">
          ❌ Error: ${data.error}
        </div>
      `;
    }
  } catch (error) {
    answerArea.innerHTML = `
      <div style="color: #ef4444; padding: 20px; text-align: center;">
        ❌ Network error: ${error.message}
      </div>
    `;
  }
}

// Allow Enter key to submit questions
document.getElementById('query-input').addEventListener('keypress', (e) => {
  if (e.key === 'Enter') {
    askQuestion();
  }
});