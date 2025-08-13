// Fixed script.js with proper async processing handling
let selectedFiles = [];
let uploadedPDFs = new Set();

// Configure marked.js for better rendering
if (typeof marked !== 'undefined') {
  marked.setOptions({
    breaks: true,
    gfm: true, // GitHub Flavored Markdown
    tables: true,
    sanitize: false,
    headerIds: false
  });
}

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
  let hasErrors = false;

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
        // FIXED: Properly handle both sync and async responses
        if (data.requires_polling) {
          // Large file - async processing
          console.log(`🔄 Large file detected: ${file.name} - Starting polling for job ${data.job_id}`);
          statusElement.textContent = '⏳ Processing in background...';
          statusElement.className = 'file-status status-processing';
          
          // Poll for status
          const finalResult = await pollProcessingStatus(data.job_id, statusElement);
          if (finalResult && finalResult.result) {
            updateFileStatusFromResult(finalResult.result, statusElement, fileName, pdfSelect);
            updateStatsFromResult(finalResult.result);
          } else {
            statusElement.textContent = '❌ Processing failed';
            statusElement.className = 'file-status status-error';
            hasErrors = true;
          }
        } else {
          // Small file - immediate result OR already cached
          console.log(`⚡ Small file or cached: ${file.name} - Processing complete`);
          updateFileStatusFromResult(data, statusElement, fileName, pdfSelect);
          updateStatsFromResult(data);
        }
      } else {
        statusElement.textContent = '❌ Error';
        statusElement.className = 'file-status status-error';
        console.error('Upload error:', data.error);
        hasErrors = true;
      }
    } catch (error) {
      statusElement.textContent = '❌ Network Error';
      statusElement.className = 'file-status status-error';
      console.error('Network error:', error);
      hasErrors = true;
    }
  }

  // Update final stats
  document.getElementById('cached-files').textContent = cachedFiles;
  document.getElementById('processed-files').textContent = processedFiles;
  document.getElementById('time-saved').textContent = `${totalTimeSaved}s`;

  // Reset UI
  uploadBtn.disabled = false;
  uploadBtn.textContent = '📤 Process Selected Files';
  progressBar.style.display = 'none';
  
  // Better UX for showing results
  if (hasErrors) {
    console.log('Some files had errors - keeping file list visible');
  } else {
    showSuccessMessage(totalFiles, cachedFiles, processedFiles);
    selectedFiles = [];
    fileInput.value = '';
    
    setTimeout(() => {
      if (confirm('Processing complete! Hide the results?')) {
        fileList.style.display = 'none';
      }
    }, 8000);
  }

  function updateFileStatusFromResult(result, statusElement, fileName, pdfSelect) {
    uploadedPDFs.add(result.pdf_name);
    
    // Update dropdown if not already present
    if (!Array.from(pdfSelect.options).some(opt => opt.value === result.pdf_name)) {
      const option = document.createElement('option');
      option.value = result.pdf_name;
      option.textContent = result.pdf_name;
      pdfSelect.appendChild(option);
    }

    if (result.cached) {
      cachedFiles++;
      statusElement.textContent = '⚡ From Cache';
      statusElement.className = 'file-status status-cached';
    } else {
      processedFiles++;
      statusElement.textContent = '✅ Processed';
      statusElement.className = 'file-status status-complete';
    }
  }

  function updateStatsFromResult(result) {
    if (result.processing_time_saved) {
      const timeSaved = parseInt(result.processing_time_saved.match(/\d+/)[0]);
      totalTimeSaved += timeSaved;
    }
  }
}

async function pollProcessingStatus(jobId, statusElement) {
  const maxAttempts = 300; // 5 minutes max (increased from 2 minutes)
  const pollInterval = 1000; // 1 second
  let lastStage = '';
  
  console.log(`🔄 Starting polling for job ${jobId}`);
  
  for (let attempt = 0; attempt < maxAttempts; attempt++) {
    try {
      const response = await fetch(`/status/${jobId}`);
      
      if (!response.ok) {
        console.error(`❌ Status check failed: HTTP ${response.status}`);
        await new Promise(resolve => setTimeout(resolve, pollInterval));
        continue;
      }
      
      const status = await response.json();
      
      // Log progress when stage changes
      if (status.stage && status.stage !== lastStage) {
        console.log(`📊 Job ${jobId}: ${status.stage}`);
        lastStage = status.stage;
      }
      
      // Update status display
      if (status.stage) {
        if (status.progress) {
          const progressPercent = Math.round(status.progress * 100);
          statusElement.textContent = `⏳ ${status.stage} (${progressPercent}%)`;
        } else {
          statusElement.textContent = `⏳ ${status.stage}`;
        }
      }
      
      // Check for completion
      if (status.status === 'completed') {
        console.log(`✅ Job ${jobId} completed successfully`);
        return status;
      } else if (status.status === 'cached') {
        console.log(`⚡ Job ${jobId} was cached`);
        return status;
      } else if (status.status === 'failed') {
        console.error(`❌ Job ${jobId} failed:`, status.error);
        return null;
      }
      
      // Continue polling
      await new Promise(resolve => setTimeout(resolve, pollInterval));
      
    } catch (error) {
      console.error(`❌ Error polling job ${jobId}:`, error);
      await new Promise(resolve => setTimeout(resolve, pollInterval));
    }
  }
  
  console.error(`❌ Polling timed out for job ${jobId} after ${maxAttempts} attempts`);
  return null;
}

function showSuccessMessage(total, cached, processed) {
  const message = document.createElement('div');
  message.className = 'success-banner';
  message.innerHTML = `
    <div style="background: linear-gradient(135deg, #10b981, #059669); color: white; padding: 15px; border-radius: 10px; margin: 10px 0;">
      🎉 <strong>Processing Complete!</strong><br>
      Total: ${total} files • Cached: ${cached} • Newly Processed: ${processed}
    </div>
  `;
  
  const fileList = document.getElementById('file-list');
  fileList.insertBefore(message, fileList.firstChild);
  
  // Remove success message after 5 seconds
  setTimeout(() => {
    if (message.parentNode) {
      message.parentNode.removeChild(message);
    }
  }, 5000);
}

function renderMarkdown(text) {
  // Check if marked.js is available
  if (typeof marked !== 'undefined') {
    return marked.parse(text);
  }
  
  // Fallback: Basic HTML formatting if marked.js isn't available
  return text
    .replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>') // Bold
    .replace(/\*(.*?)\*/g, '<em>$1</em>') // Italic
    .replace(/### (.*?)$/gm, '<h3>$1</h3>') // H3 headers
    .replace(/## (.*?)$/gm, '<h2>$1</h2>') // H2 headers
    .replace(/# (.*?)$/gm, '<h1>$1</h1>') // H1 headers
    .replace(/\n\n/g, '</p><p>') // Paragraphs
    .replace(/\n/g, '<br>') // Line breaks
    .replace(/^\s*[-*+]\s+(.*?)$/gm, '<li>$1</li>') // List items
    .replace(/(<li>.*<\/li>)/gs, '<ul>$1</ul>') // Wrap lists
    .replace(/\|(.+)\|/g, (match, content) => { // Tables (basic)
      const cells = content.split('|').map(cell => `<td>${cell.trim()}</td>`).join('');
      return `<tr>${cells}</tr>`;
    });
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
      // Display answer with proper markdown rendering
      const renderedAnswer = renderMarkdown(data.answer);
      
      answerArea.innerHTML = `
        <div class="answer-content">
          ${renderedAnswer}
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