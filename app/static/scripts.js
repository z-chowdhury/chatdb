document.getElementById('queryForm').addEventListener('submit', function(event) {
    event.preventDefault();

    let formData = new FormData(this);
    fetch('/submit_query', {
        method: 'POST',
        body: new URLSearchParams(formData)
    })
    .then(response => response.json())
    .then(data => {
        console.log("Response data:", data);  // Log the entire response to check its structure

        let queryResults = document.getElementById('query-results');
        queryResults.style.display = 'block';
        document.getElementById('generatedQuery').textContent = JSON.stringify(data.generated_query, null, 2);

        let resultBody = document.getElementById('resultBody');
        resultBody.innerHTML = ''; // Clear previous results

        if (data.result && data.result.length > 0) {
            // Dynamically create the header row based on keys of the first result object
            let resultTable = queryResults.querySelector('table');
            let resultHeader = resultTable.querySelector('thead');
            resultHeader.innerHTML = ''; // Clear previous headers

            let headerRow = document.createElement('tr');
            Object.keys(data.result[0]).forEach(key => {
                let th = document.createElement('th');
                th.textContent = key;
                headerRow.appendChild(th);
            });
            resultHeader.appendChild(headerRow);

            // Populate table rows with the result data
            data.result.forEach(row => {
                let tr = document.createElement('tr');
                Object.values(row).forEach(value => {
                    let td = document.createElement('td');
                    td.textContent = value;
                    tr.appendChild(td);
                });
                resultBody.appendChild(tr);
            });
        } else {
            console.error("No results found.");
            document.getElementById('generatedQuery').textContent = "No results found for your query.";
        }

        // Scroll to the results section
        setTimeout(() => {
            queryResults.scrollIntoView({ behavior: 'smooth', block: 'start' });
        }, 200); // Add a slight delay to ensure the results are displayed before scrolling
    })
    .catch(error => console.error('Error:', error));  // Handle any errors
});

