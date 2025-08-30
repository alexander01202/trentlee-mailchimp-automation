EMAIL_TEMPLATE = """
<!DOCTYPE html>
        <html>
        <head>
            <meta charset="utf-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>New Business Listings</title>
        </head>
        <body style="font-family: Arial, sans-serif; line-height: 1.6; margin: 0; padding: 20px; background-color: #f4f4f4;">
            <div style="max-width: 800px; margin: 0 auto; background-color: white; padding: 30px; border-radius: 10px; box-shadow: 0 0 10px rgba(0,0,0,0.1); border-bottom: 2px solid #007cba;">
                <p></p>
                <p class="mcePastedContent">Hi {first_name},</p>
                <p class="mcePastedContent"></p>
                <p class="mcePastedContent">Good news — a new business just came on the market that matches your buying criteria. I am posting the details on the matching busines below this section. </p>
                <p class="mcePastedContent"></p>
                <p class="mcePastedContent"><strong>Here’s what you need to do next:</strong><br>To get the full details, you’ll need to reach out to the listing broker directly and sign an NDA. That’s standard for every deal. Once you do, they’ll send you the financials, details, and access to ask your questions.</p>
                <p class="mcePastedContent"></p>
                <p class="mcePastedContent">If you’d like, I’d be happy to represent you as your buyer’s broker. That way I can help you evaluate the deal, negotiate terms, and guide you through financing and closing — all at no cost to you (the seller pays the commission). If you’re interested, just hit reply to this email and I’ll walk you through how I can help.</p>
                <p class="mcePastedContent"></p>
                <p class="mcePastedContent"><strong>Extra help for first-time buyers:</strong><br>If you’re buying a business for the first time, I strongly recommend checking out my <strong>free 30+ hour video course</strong> and joining our private <strong>Skool community</strong>. It’s a complete step-by-step training on how to buy a business the right way, plus a network of active buyers you can learn from. It’s free, and it will save you from costly mistakes.</p>
                <p class="mcePastedContent"></p>
                <p class="mcePastedContent">Here’s the link to access it: https://businessbrokerslasvegas.net/business-exit-acquisition-playbook-free-course/</p>
                <p class="mcePastedContent"></p>
                <p class="mcePastedContent">Timing is everything in this market. If this listing is a fit, move quickly before another buyer ties it up.</p>
            </div>
            
             <!-- Listings -->
            <div style="margin: 30px 0;">
                {listings_html}
            </div>
        </body>
        </html>
"""