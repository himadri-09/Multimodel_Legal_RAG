// script.js
async function uploadPDF() {
  const fileInput = document.getElementById("pdf-upload");
  const status = document.getElementById("upload-status");
  const pdfSelect = document.getElementById("pdf-select");

  if (!fileInput.files.length) {
    status.textContent = "Please select a PDF.";
    return;
  }

  const formData = new FormData();
  formData.append("file", fileInput.files[0]);

  status.textContent = "Uploading and processing...";
  try {
    const res = await fetch("/upload", {
      method: "POST",
      body: formData,
    });
    const data = await res.json();
    if (res.ok) {
      status.textContent = `✅ ${data.pdf_name} uploaded and processed!`;
      const opt = document.createElement("option");
      opt.value = data.pdf_name;
      opt.textContent = data.pdf_name;
      pdfSelect.appendChild(opt);
    } else {
      status.textContent = `❌ Error: ${data.error}`;
    }
  } catch (err) {
    status.textContent = `❌ Network error: ${err.message}`;
  }
}

async function askQuestion() {
  const query = document.getElementById("query-input").value.trim();
  const pdfName = document.getElementById("pdf-select").value;
  const answerDiv = document.getElementById("answer");
  const imagesDiv = document.getElementById("images");

  if (!query) {
    alert("Please enter a question.");
    return;
  }

  answerDiv.textContent = "Thinking...";
  imagesDiv.innerHTML = "";

  try {
    const res = await fetch("/query", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ query, pdf_name: pdfName }),
    });
    const data = await res.json();
    if (res.ok) {
      answerDiv.innerHTML = `<p><strong>Answer:</strong> ${data.answer.replace(/\n/g, "<br>")}</p>`;
      if (data.images && data.images.length > 0) {
        data.images.forEach(img => {
          const imgEl = document.createElement("img");
          imgEl.src = img.url;
          imgEl.alt = img.caption;
          imgEl.style.maxWidth = "100%";
          imgEl.style.margin = "10px 0";
          imagesDiv.appendChild(imgEl);
          const caption = document.createElement("p");
          caption.innerHTML = `<small><strong>Image (Page ${img.page}):</strong> ${img.caption}</small>`;
          imagesDiv.appendChild(caption);
        });
      }
    } else {
      answerDiv.textContent = `Error: ${data.error}`;
    }
  } catch (err) {
    answerDiv.textContent = `Network error: ${err.message}`;
  }
}