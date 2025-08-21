import json, os, asyncio
from webcrawler import CpsScraper


def main():
    data = asyncio.run(
        CpsScraper.nuxt_to_data(
            "https://cellphones.com.vn/zte-blade-a52.html", encoding="utf-8"
        )
    )

    print(data["fetch"]["product-detail:0"]["variants"][0]["general"])


if __name__ == "__main__":
    main()
