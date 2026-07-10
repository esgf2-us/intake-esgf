import signal
import sys
import time
from threading import Event

done_event = Event()


def handle_sigint(signum, frame):
    print(f"\nTutorial interrupted by {signum}, shutting down.")
    done_event.set()
    exit()


signal.signal(signal.SIGINT, handle_sigint)
signal.signal(signal.SIGTERM, handle_sigint)

if __name__ == "__main__":
    sentences = [
        "Hello and thanks for trying out intake-esgf!",
        "We wrote this tool especially for climate scientists interested in analysis.",
        "This ~5 min tutorial will walk you through the basics.",
        "To exit this tutorial at any point, press <CTRL>-C.",
        "To get started you will need to import the catalog from the package.",
        ">>> from intake_esgf import ESGFCatalog",
        "Then instantiate a new catalog.",
        ">>> cat = ESGFCatalog()",
        "You can print the catalog to see a summary of what is inside.",
        ">>> print(cat)",
        "Catalogs initialize empty though.",
        "Let's populate it by using the search function.",
        ">>> cat.search(experiment_id='historical',source_id='CanESM5',variable_id=['pr','tas'],frequency='mon')",
        "This sends a query to ESGF using the given facets and values.",
        "Print the catalog again to see what we found.",
        ">>> print(cat)",
        "Printing the catalog shows the facets of the search results and unique values found.",
        "Oops! We forgot to include a member_id and got too many results.",
        "Searches are non-cumulative so repeat with a member_id.",
        ">>> cat.search(experiment_id='historical',source_id='CanESM5',variable_id=['pr','tas'],frequency='mon',member_id='r1i1p1f1')",
        ">>> print(cat)",
        "That looks better! Now we are ready to download!",
        "The catalog has a function that will download your data...",
        "...and then load it into a dictionary of xarray datasets.",
        ">>> dsd = cat.to_dataset_dict()",
        "That actually downloaded data to your system.",
        "But it will only do it once. If you run it again...",
        ">>> dsd = cat.to_dataset_dict()",
        "...we detect you already have these files and simply load them.",
        "Let's look at the keys of this dictionary",
        ">>> print(dsd.keys())",
        "By default the keys will be composed of the facet values that are different in your catalog.",
        "You can change this behavior. For example...",
        ">>> dsd = cat.to_dataset_dict(minimal_keys=False)",
        ">>> print(dsd.keys())",
        "Now the keys are built using all the facets.",
        "Let's examine what is in this dictionary.",
        ">>> print(dsd['CMIP6.CMIP.CCCma.CanESM5.historical.r1i1p1f1.Amon.pr.gn'])",
        "The files were downloaded to a local cache on your system.",
        "This will default to a hidden directory in your home folder, but you can change it.",
        "Note we loaded these files into xarray datasets for you.",
        "They are ready for your analysis operations.",
        "If you don't want to use xarray, you could call 'cat.to_path_dict()' instead.",
        "This will still download files but returns a dictionary of paths instead.",
        "Also look again at the data arrays in the dataset.",
        "You will see that we detected that 'pr' uses 'areacella' as a cell measure.",
        "We found 'areacella' for you and already merged it into your dataset.",
        "We hope that the intake-esgf code needed to download and load data...",
        "...is so short and fast, that you will just leave it in your analysis code.",
        "This makes your scripts portable and reproducible.",
        "You can send them to colleagues and they can grab the exact data you used.",
        "intake-esgf has many other features designed to help you answer science questions.",
        "We hope that you will check it out!",
        "We can't wait to see the new insights you will have!",
    ]
    type_delay = 0.01
    delete_delay = 0.005
    read_delay_per_char = 0.08

    print(chr(27) + "[2J")
    print("""
 o8o                  .             oooo                                                         .o88o.
 `"'                .o8             `888                                                         888 `"
oooo  ooo. .oo.   .o888oo  .oooo.    888  oooo   .ooooo.           .ooooo.   .oooo.o  .oooooooo o888oo
`888  `888P"Y88b    888   `P  )88b   888 .8P'   d88' `88b         d88' `88b d88(  "8 888' `88b   888
 888   888   888    888    .oP"888   888888.    888ooo888 8888888 888ooo888 `"Y88b.  888   888   888
 888   888   888    888 . d8(  888   888 `88b.  888    .o         888    .o o.  )88b `88bod8P'   888
o888o o888o o888o   "888" `Y888""8o o888o o888o `Y8bod8P'         `Y8bod8P' 8""888P' `8oooooo.  o888o
                                                                                     d"     YD
                                                                                     "Y88888P'\n""")
    for sentence in sentences:
        read_delay = read_delay_per_char * len(sentence)
        for char in sentence:
            sys.stdout.write(char)
            sys.stdout.flush()
            time.sleep(type_delay)
        if sentence.startswith(">>> "):
            sys.stdout.write("\n")
            exec(sentence.replace(">>> ", ""))
            sys.stdout.write("\n")
            sys.stdout.flush()
            time.sleep(read_delay)
        else:
            time.sleep(read_delay)
            for _ in sentence:
                sys.stdout.write("\b \b")
                sys.stdout.flush()
                time.sleep(delete_delay)
        print("\r", end="")

    sentence = "For more information about intake-esgf, please visit https://intake-esgf.readthedocs.io/en/latest/\n\n"
    for char in sentence:
        sys.stdout.write(char)
        sys.stdout.flush()
        time.sleep(type_delay)
