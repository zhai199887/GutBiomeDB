import classes from "./Footer.module.css";

const Footer = () => (
  <footer>
    <p>
      A project of the <strong>Gastroenterology Laboratory</strong>,{" "}
      <a href="https://www.cmu.edu.cn" target="_blank" rel="noreferrer">
        China Medical University
      </a>
      .
    </p>

    <div className={classes.cols}>
      <a
        href="https://www.cmu.edu.cn"
        target="_blank"
        rel="noreferrer"
        data-tooltip="China Medical University · Gastroenterology Laboratory"
        className={classes.logoBlock}
      >
        <div className={classes.logoMark} aria-hidden="true">
          <span>CMU</span>
        </div>
        <div className={classes.logoText}>
          <span className={classes.logoEn}>China Medical University</span>
          <span className={classes.logoCn}>中国医科大学</span>
          <span className={classes.logoDept}>Gastroenterology Laboratory</span>
        </div>
      </a>
    </div>
  </footer>
);

export default Footer;
