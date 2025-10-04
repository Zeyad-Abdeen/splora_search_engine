document.addEventListener('DOMContentLoaded', () => {
    const API_BASE_URL = 'http://127.0.0.1:8080';

    // --- THEME (DARK/LIGHT MODE) TOGGLE LOGIC ---
    const themeToggleButtons = document.querySelectorAll('.theme-toggle-btn');
    const applyTheme = (theme) => {
        document.documentElement.classList.toggle('dark', theme === 'dark');
        themeToggleButtons.forEach(button => {
            const lightIcon = button.querySelector('.theme-toggle-light-icon');
            const darkIcon = button.querySelector('.theme-toggle-dark-icon');
            if (lightIcon && darkIcon) {
                lightIcon.style.display = (theme === 'dark') ? 'none' : 'inline-block';
                darkIcon.style.display = (theme === 'dark') ? 'inline-block' : 'none';
            }
        });
    };
    const savedTheme = localStorage.getItem('theme') || (window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light');
    applyTheme(savedTheme);
    themeToggleButtons.forEach(button => {
        button.addEventListener('click', () => {
            const newTheme = document.documentElement.classList.contains('dark') ? 'light' : 'dark';
            localStorage.setItem('theme', newTheme);
            applyTheme(newTheme);
        });
    });

    // --- HOME PAGE SEARCH LOGIC ---
    if (window.location.pathname.includes('home_page')) {
        const homeSearchForm = document.querySelector('.home-search-form');
        if (homeSearchForm) {
            homeSearchForm.addEventListener('submit', (e) => {
                e.preventDefault();
                const query = homeSearchForm.querySelector('.home-search-input').value.trim();
                if (query) {
                    window.location.href = `../search_and_results_page/code.html?q=${encodeURIComponent(query)}`;
                }
            });
        }
    }

    // --- SEARCH & RESULTS PAGE LOGIC (PAGINATED) ---
    if (window.location.pathname.includes('search_and_results_page')) {
        const searchInput = document.querySelector('input[placeholder*="Search"]');
        const searchButton = document.querySelector('button.bg-primary');

        const resultsContainer = document.querySelector('.grid.grid-cols-1');
        const resultsCountEl = document.querySelector('p.text-2xl');
        const nextPageBtn = document.getElementById('next-page-btn');
        const previousPageBtn = document.getElementById('previous-page-btn');
        
        let currentPage = 1;
        let currentQuery = '';
        let currentResults = [];

        const performSearch = async (query, page = 1) => {
            if (!query) return;
            
            currentPage = page;
            currentQuery = query;

            resultsCountEl.textContent = `Engaging the AI core... Analyzing 608 NASA publications across deep space and time. Thanks for your patience while we pinpoint the most relevant discoveries.`;
            nextPageBtn.classList.add('hidden');
            previousPageBtn.classList.add('hidden');

            try {
                // Build the API URL
                let apiUrl = `${API_BASE_URL}/search?q=${encodeURIComponent(query)}&page=${currentPage}&num_results=20`;

                
                const response = await fetch(apiUrl);
                if (!response.ok) throw new Error(`API error! Status: ${response.status}`);
                
                const data = await response.json();
                displayResults(data.results);

            } catch (error) {
                console.error("Search failed:", error);
                resultsContainer.innerHTML = `<p class="col-span-full text-center p-8 text-red-500">Failed to load page ${currentPage}.</p>`;
            }
        };

        const displayResults = (results) => {
            currentResults = results;
            resultsContainer.innerHTML = ''; 

            if (results.length === 0 && currentPage === 1) {
                resultsCountEl.textContent = `No results found for '${currentQuery}'`;
                return;
            }

            // Get currently saved articles to check against
            const savedArticles = JSON.parse(localStorage.getItem('savedArticles')) || [];
            const savedDocIds = new Set(savedArticles.map(a => a.doc_id));

            results.forEach(result => {
                let authorsDisplay = result.authors.slice(0, 9).join(', ');
                if (result.authors.length > 9) authorsDisplay += ', et al.';

                // Check if the current result is already saved
                const isSaved = savedDocIds.has(result.doc_id);
                const saveButtonText = isSaved ? 'Saved!' : 'Save';
                const saveButtonDisabled = isSaved ? 'disabled' : '';
                
                const articleCard = `
                    <div class="flex flex-col gap-4 rounded-xl p-6 bg-card-light dark:bg-card-dark border shadow-sm">
                        <a href="${result.url}" target="_blank" rel="noopener noreferrer"><h4 class="text-lg font-bold text-primary hover:underline">${result.title}</h4></a>
                        <p class="text-sm text-gray-400">${authorsDisplay}</p>
                        <p class="text-sm text-gray-500">${result.publication_date}</p>
                        <p class="text-sm line-clamp-3">${result.abstract}</p>
                        <div class="flex items-center gap-4 mt-auto pt-4">
                            <button class="summarize-btn bg-accent text-white font-semibold py-2 px-4 rounded-lg text-sm flex-1" data-url="${result.url}" data-title="${result.title}">Summarize</button>
                            <button class="save-btn bg-transparent border border-accent text-accent font-semibold py-2 px-4 rounded-lg text-sm" data-doc-id="${result.doc_id}" ${saveButtonDisabled}>${saveButtonText}</button>
                        </div>
                    </div>`;
                resultsContainer.insertAdjacentHTML('beforeend', articleCard);
            });

            resultsCountEl.textContent = `Showing Page ${currentPage} for '${currentQuery}'`;
            nextPageBtn.classList.toggle('hidden', results.length < 20);
            previousPageBtn.classList.toggle('hidden', currentPage <= 1);
        };

        const handleNewSearch = () => {
            const query = searchInput.value.trim();
            performSearch(query, 1);
        };

        searchButton.addEventListener('click', handleNewSearch);
        searchInput.addEventListener('keyup', (e) => {
            if (e.key === 'Enter') handleNewSearch();
        });

        nextPageBtn.addEventListener('click', () => {
            performSearch(currentQuery, currentPage + 1);
        });
        
        previousPageBtn.addEventListener('click', () => {
            performSearch(currentQuery, currentPage - 1);
        });
        


        resultsContainer.addEventListener('click', async (e) => {
            // --- SUMMARIZE BUTTON LOGIC ---
            if (e.target.classList.contains('summarize-btn')) {
                const button = e.target;
                const url = button.dataset.url;
                const title = button.dataset.title;

                // Show the modal and set initial state
                const modal = document.getElementById('summary-modal');
                const modalTitle = document.getElementById('modal-title');
                const modalSummary = document.getElementById('modal-summary');
                const modalUrl = document.getElementById('modal-url');

                modalTitle.textContent = title;
                modalUrl.href = url;
                modalSummary.textContent = 'Generating summary...';
                modal.classList.remove('hidden');

                try {
                    const response = await fetch(`${API_BASE_URL}/summarise`, {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json',
                        },
                        body: JSON.stringify({
                            url: url,
                            title: title
                        })
                    });

                    if (!response.ok) {
                        throw new Error('Failed to fetch summary.');
                    }

                    const data = await response.json();

                    if (data.success) {
                        modalSummary.innerHTML = marked.parse(data.summary);
                    } else {
                        throw new Error('Summary generation failed.');
                    }

                } catch (error) {
                    console.error("Summarization failed:", error);
                    modalSummary.textContent = 'Could not generate a summary for this article.';
                }
            }

            // --- SAVE BUTTON LOGIC (WITH DEBUGGING) ---
            if (e.target.classList.contains('save-btn')) {
                console.log('--- Save Process Started ---');
                
                const button = e.target;
                const docId = button.dataset.docId;
                console.log('Step 1: docId from button attribute is:', docId, `(Type: ${typeof docId})`);

                // Check if currentResults is available and has content
                if (!currentResults || currentResults.length === 0) {
                    console.error('ERROR: The `currentResults` array is empty or not available!');
                    return; // Stop execution if there's nothing to search in
                }
                console.log('Step 2: Searching for this docId in the `currentResults` array which has', currentResults.length, 'items.');

                // Find the article
                const articleToSave = currentResults.find(r => {
                    // This comparison is critical
                    return String(r.doc_id) === String(docId); 
                });

                if (articleToSave) {
                    console.log('Step 3: SUCCESS - Found the article object:', articleToSave);
                    
                    const savedArticles = JSON.parse(localStorage.getItem('savedArticles')) || [];
                    console.log('Step 4: Current articles in localStorage:', savedArticles);

                    // Check if article is already saved
                    if (!savedArticles.some(a => a.doc_id === articleToSave.doc_id)) {
                        savedArticles.push(articleToSave);
                        localStorage.setItem('savedArticles', JSON.stringify(savedArticles));
                        console.log('Step 5: SUCCESS - Article has been added. New localStorage content:', JSON.parse(localStorage.getItem('savedArticles')));
                        button.textContent = 'Saved!';
                        button.disabled = true;
                    } else {
                        console.log('Step 5: INFO - Article was already saved.');
                        button.textContent = 'Already Saved';
                        button.disabled = true;
                    }
                } else {
                    console.error('Step 3: FAILED - Could not find an article with doc_id', docId, 'in the `currentResults` array.');
                }
                console.log('--- Save Process Finished ---');
            }
        });

        // --- MODAL CLOSE LOGIC ---
        const modal = document.getElementById('summary-modal');
        const modalCloseBtn = document.getElementById('modal-close');
        modalCloseBtn.addEventListener('click', () => {
            modal.classList.add('hidden');
        });

        const urlParams = new URLSearchParams(window.location.search);
        const queryFromUrl = urlParams.get('q');
        if (queryFromUrl) {
            searchInput.value = decodeURIComponent(queryFromUrl);
            performSearch(queryFromUrl, 1);
        }
    }

    // --- SUMMARY MODAL LOGIC (Unchanged) ---
    // (Your existing summary modal code here)

    // --- SAVED ARTICLES PAGE LOGIC ---
    if (window.location.pathname.includes('saved_articles_page')) {
        const savedArticlesContainer = document.getElementById('saved-articles-container');
        let savedArticles = JSON.parse(localStorage.getItem('savedArticles')) || [];

        const displaySavedArticles = () => {
            savedArticlesContainer.innerHTML = '';
            if (savedArticles.length === 0) {
                savedArticlesContainer.innerHTML = '<p class="col-span-full text-center p-8">You have no saved articles.</p>';
                return;
            }

            savedArticles.forEach(result => {
                let authorsDisplay = result.authors.slice(0, 9).join(', ');
                if (result.authors.length > 9) authorsDisplay += ', et al.';

                const articleCard = `
                    <div class="flex flex-col gap-4 rounded-xl p-6 bg-card-light dark:bg-card-dark border shadow-sm" data-doc-id="${result.doc_id}">
                        <a href="${result.url}" target="_blank" rel="noopener noreferrer"><h4 class="text-lg font-bold text-primary hover:underline">${result.title}</h4></a>
                        <p class="text-sm text-gray-400">${authorsDisplay}</p>
                        <p class="text-sm text-gray-500">${result.publication_date}</p>
                        <p class="text-sm line-clamp-3">${result.abstract}</p>
                        <div class="flex items-center gap-4 mt-auto pt-4">
                            <button class="summarize-btn bg-accent text-white font-semibold py-2 px-4 rounded-lg text-sm flex-1" data-url="${result.url}" data-title="${result.title}">Summarize</button>
                            <button class="remove-btn bg-transparent border border-accent text-accent font-semibold py-2 px-4 rounded-lg text-sm" data-doc-id="${result.doc_id}">Remove</button>
                        </div>
                    </div>`;
                savedArticlesContainer.insertAdjacentHTML('beforeend', articleCard);
            });
        }

        displaySavedArticles();

        savedArticlesContainer.addEventListener('click', async (e) => {
            // --- SUMMARIZE BUTTON LOGIC ---
            if (e.target.classList.contains('summarize-btn')) {
                const button = e.target;
                const url = button.dataset.url;
                const title = button.dataset.title;

                // Show the modal and set initial state
                const modal = document.getElementById('summary-modal');
                const modalTitle = document.getElementById('modal-title');
                const modalSummary = document.getElementById('modal-summary');
                const modalUrl = document.getElementById('modal-url');

                modalTitle.textContent = title;
                modalUrl.href = url;
                modalSummary.textContent = 'Generating summary...';
                modal.classList.remove('hidden');

                try {
                    const response = await fetch(`${API_BASE_URL}/summarise`, {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json',
                        },
                        body: JSON.stringify({
                            url: url,
                            title: title
                        })
                    });

                    if (!response.ok) {
                        throw new Error('Failed to fetch summary.');
                    }

                    const data = await response.json();

                    if (data.success) {
                        modalSummary.innerHTML = marked.parse(data.summary);
                    } else {
                        throw new Error('Summary generation failed.');
                    }

                } catch (error) {
                    console.error("Summarization failed:", error);
                    modalSummary.textContent = 'Could not generate a summary for this article.';
                }
            }

            // --- REMOVE BUTTON LOGIC ---
            if (e.target.classList.contains('remove-btn')) {
                const button = e.target;
                const docIdToRemove = button.dataset.docId;

                // Remove from localStorage
                savedArticles = savedArticles.filter(article => String(article.doc_id) !== docIdToRemove);
                localStorage.setItem('savedArticles', JSON.stringify(savedArticles));

                // Remove from DOM
                const articleCard = button.closest('[data-doc-id]');
                if (articleCard) {
                    articleCard.remove();
                }

                if (savedArticles.length === 0) {
                    savedArticlesContainer.innerHTML = '<p class="col-span-full text-center p-8">You have no saved articles.</p>';
                }
            }
        });

        // --- MODAL CLOSE LOGIC ---
        const modal = document.getElementById('summary-modal');
        const modalCloseBtn = document.getElementById('modal-close');
        if (modal && modalCloseBtn) {
            modalCloseBtn.addEventListener('click', () => {
                modal.classList.add('hidden');
            });
        }
    }
});